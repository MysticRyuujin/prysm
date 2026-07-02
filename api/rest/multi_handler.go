package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/OffchainLabs/prysm/v7/network/httputil"
)

type multiHandler struct {
	handlers []*handler
}

// sszResult carries an SSZ-preferred response body and its headers across the
// GET/POST SSZ race helpers.
type sszResult struct {
	body   []byte
	header http.Header
}

// newMultiHandler builds a multiHandler over the given per-endpoint handlers.
func newMultiHandler(handlers []*handler) (*multiHandler, error) {
	if len(handlers) == 0 {
		return nil, errors.New("multiHandler requires at least one handler")
	}

	return &multiHandler{handlers: handlers}, nil
}

// Host returns a representative endpoint (the first) for logging purposes.
func (m *multiHandler) Host() string {
	return m.handlers[0].Host()
}

// Get reads a GET from the nodes. Without a WithAccept option it tries nodes in
// order and decodes the first successful response into resp (primary, failing
// over only on error). With WithAccept it selects across nodes for a response
// satisfying the predicate.
func (m *multiHandler) Get(ctx context.Context, endpoint string, resp any, opts ...GetOption) error {
	cfg := newGetConfig(opts)

	get := func(ctx context.Context, handler *handler) (json.RawMessage, error) {
		// We don't care about the response body if resp is nil.
		if resp == nil {
			if err := handler.Get(ctx, endpoint, nil); err != nil {
				return nil, fmt.Errorf("get: %w", err)
			}

			return nil, nil
		}

		// We do care about the response body.
		raw, err := handler.getRaw(ctx, endpoint)
		if err != nil {
			return nil, fmt.Errorf("get: %w", err)
		}

		return raw, nil
	}

	// Race-until-match path.
	if cfg.accept != nil && (len(m.handlers) > 1 || cfg.repoll) {
		raw, _, err := raceReadUntil(ctx, m.handlers, cfg, get)
		if err != nil {
			return fmt.Errorf("raceReadUntil: %w", err)
		}

		return decodeInto(raw, resp)
	}

	// First success path.
	if len(m.handlers) == 1 {
		if err := m.handlers[0].Get(ctx, endpoint, resp); err != nil {
			return fmt.Errorf("get: %w", err)
		}

		return nil
	}

	raw, err := firstSuccess(ctx, m.handlers, get)
	if err != nil {
		return fmt.Errorf("firstSuccess: %w", err)
	}

	return decodeInto(raw, resp)
}

// GetSSZ reads a GET (SSZ-preferred) from the nodes. Without a WithSSZAccept
// option it tries nodes in order and returns the first success (primary,
// failing over only on error). With WithSSZAccept it races the nodes and
// prefers a response satisfying the predicate.
func (m *multiHandler) GetSSZ(ctx context.Context, endpoint string, opts ...GetOption) ([]byte, http.Header, error) {
	cfg := newGetConfig(opts)

	get := func(ctx context.Context, h *handler) (sszResult, error) {
		body, header, err := h.GetSSZ(ctx, endpoint)
		if err != nil {
			return sszResult{}, fmt.Errorf("get ssz: %w", err)
		}

		return sszResult{body: body, header: header}, nil
	}

	race := firstSuccess[sszResult]
	if cfg.sszAccept != nil && len(m.handlers) > 1 {
		accept := func(r sszResult) bool { return cfg.sszAccept(r.body, r.header) }
		race = func(ctx context.Context, handlers []*handler, fn func(context.Context, *handler) (sszResult, error)) (sszResult, error) {
			return raceReadAccept(ctx, handlers, cfg.deadline, accept, fn)
		}
	}

	res, err := race(ctx, m.handlers, get)
	if err != nil {
		return nil, nil, err
	}

	return res.body, res.header, nil
}

// GetStatusCode queries all nodes and prefers a 200 (ready): it returns 200 if
// any node is ready, otherwise the last non-200 status observed, or a joined
// error if every node failed at the transport level.
func (m *multiHandler) GetStatusCode(ctx context.Context, endpoint string) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		code int
		err  error
	}

	results := make(chan result, len(m.handlers))
	for _, h := range m.handlers {
		go func(h *handler) {
			code, err := h.GetStatusCode(ctx, endpoint)
			results <- result{code: code, err: err}
		}(h)
	}

	var (
		lastCode int
		errs     []error
	)

	for range m.handlers {
		select {
		case result := <-results:
			if result.err != nil {
				errs = append(errs, result.err)
				continue
			}

			if result.code == http.StatusOK {
				return http.StatusOK, nil
			}

			lastCode = result.code
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	if lastCode != 0 {
		return lastCode, nil
	}

	return 0, errors.Join(errs...)
}

// Post broadcasts a POST to all nodes and succeeds as soon as one node accepts.
// The first successful response is decoded into resp.
func (m *multiHandler) Post(ctx context.Context, endpoint string, headers map[string]string, data *bytes.Buffer, resp any) error {
	if len(m.handlers) == 1 {
		if err := m.handlers[0].Post(ctx, endpoint, headers, data, resp); err != nil {
			return fmt.Errorf("post: %w", err)
		}

		return nil
	}

	raw := []byte{}
	if data != nil {
		raw = data.Bytes()
	}

	post := func(ctx context.Context, h *handler) (json.RawMessage, error) {
		// We don't care about the response body if resp is nil.
		if resp == nil {
			if err := h.Post(ctx, endpoint, headers, cloneBuffer(data, raw), nil); err != nil {
				return nil, fmt.Errorf("post: %w", err)
			}

			return nil, nil
		}

		// We do care about the response body.
		var out json.RawMessage
		if err := h.Post(ctx, endpoint, headers, cloneBuffer(data, raw), &out); err != nil {
			return nil, fmt.Errorf("post: %w", err)
		}

		return out, nil
	}

	out, err := broadcastWrite(ctx, m.handlers, post)
	if err != nil {
		return fmt.Errorf("broadcastWrite: %w", err)
	}

	return decodeInto(out, resp)
}

// PostSSZ broadcasts an SSZ-preferred POST to all nodes and returns the first
// successful response.
func (m *multiHandler) PostSSZ(ctx context.Context, endpoint string, headers map[string]string, data *bytes.Buffer) ([]byte, http.Header, error) {
	if len(m.handlers) == 1 {
		result, header, err := m.handlers[0].PostSSZ(ctx, endpoint, headers, data)
		if err != nil {
			return nil, nil, fmt.Errorf("post ssz: %w", err)
		}

		return result, header, nil
	}

	var raw []byte
	if data != nil {
		raw = data.Bytes()
	}

	post := func(ctx context.Context, h *handler) (sszResult, error) {
		body, hdr, err := h.PostSSZ(ctx, endpoint, headers, cloneBuffer(data, raw))
		if err != nil {
			return sszResult{}, fmt.Errorf("post ssz: %w", err)
		}

		return sszResult{body: body, header: hdr}, nil
	}

	vals, errs := broadcastWriteAll(ctx, m.handlers, post)
	for _, err := range errs {
		if errors.Is(err, &httputil.DefaultJsonError{Code: http.StatusNotAcceptable}) {
			return nil, nil, err
		}
	}

	if len(vals) > 0 {
		return vals[0].body, vals[0].header, nil
	}

	return nil, nil, errors.Join(errs...)
}

// firstSuccess tries each handler in order and returns the first success.
func firstSuccess[T any](ctx context.Context, handlers []*handler, fn func(context.Context, *handler) (T, error)) (T, error) {
	var errs []error
	for _, h := range handlers {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, err
		}

		val, err := fn(ctx, h)
		if err == nil {
			return val, nil
		}

		errs = append(errs, err)
	}

	var zero T
	return zero, errors.Join(errs...)
}

// raceReadAccept runs fn against every handler concurrently and returns the
// first result satisfying accept.
func raceReadAccept[T any](ctx context.Context, handlers []*handler, deadline time.Time, accept func(T) bool, fn func(context.Context, *handler) (T, error)) (T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		val T
		err error
	}

	results := make(chan result, len(handlers))
	for _, h := range handlers {
		go func(h *handler) {
			val, err := fn(ctx, h)
			results <- result{val: val, err: err}
		}(h)
	}

	var (
		fallback     T
		haveFallback bool
		errs         []error
	)

	budget, budgetElapsed, stopBudget := freshnessBudget(deadline)
	defer stopBudget()

	for received := 0; received < len(handlers); {
		select {
		case r := <-results:
			received++
			if r.err != nil {
				errs = append(errs, r.err)
				break
			}

			if accept(r.val) {
				return r.val, nil
			}

			if !haveFallback {
				fallback, haveFallback = r.val, true
			}

			if budgetElapsed {
				return fallback, nil
			}
		case <-budget:
			budget = nil
			budgetElapsed = true
			if haveFallback {
				return fallback, nil
			}
		case <-ctx.Done():
			if haveFallback {
				return fallback, nil
			}

			var zero T
			return zero, ctx.Err()
		}
	}

	if haveFallback {
		return fallback, nil
	}

	var zero T
	return zero, errors.Join(errs...)
}

func freshnessBudget(deadline time.Time) (budget <-chan time.Time, elapsed bool, stop func()) {
	if deadline.IsZero() {
		return nil, true, func() {}
	}

	timer := time.NewTimer(time.Until(deadline))
	return timer.C, false, func() { timer.Stop() }
}

// raceReadUntil polls fn against every handler concurrently, in rounds, and
// returns the first response that satisfies cfg.accept. Once cfg.deadline
// passes with no match, the most recently received successful response is
// returned as a best-effort fallback.
func raceReadUntil(ctx context.Context, handlers []*handler, cfg getConfig, fn func(context.Context, *handler) (json.RawMessage, error)) (json.RawMessage, bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		val json.RawMessage
		err error
	}

	var fallbackResponse json.RawMessage
	haveFallback := false
	for {
		roundCtx := ctx
		roundCancel := func() {}
		if !cfg.deadline.IsZero() {
			roundCtx, roundCancel = context.WithDeadline(ctx, cfg.deadline)
		}

		results := make(chan result, len(handlers))
		for _, h := range handlers {
			go func(h *handler) {
				val, err := fn(roundCtx, h)
				results <- result{val: val, err: err}
			}(h)
		}

		var errs []error
		for range handlers {
			select {
			case result := <-results:
				if result.err != nil {
					errs = append(errs, result.err)
					continue
				}

				if cfg.accept(result.val) {
					roundCancel()
					return result.val, true, nil
				}

				fallbackResponse, haveFallback = result.val, true
			case <-ctx.Done():
				roundCancel()
				if haveFallback {
					return fallbackResponse, false, nil
				}

				return nil, false, ctx.Err()
			}
		}

		roundCancel()

		// No match this round. Stop after this single best-effort round unless
		// re-polling is enabled. When it is, stop once the deadline has passed
		// or the context is done.
		if ctx.Err() != nil || !cfg.repoll || cfg.deadline.IsZero() || !time.Now().Before(cfg.deadline) {
			if haveFallback {
				return fallbackResponse, false, nil
			}

			return nil, false, errors.Join(errs...)
		}

		select {
		case <-ctx.Done():
			if haveFallback {
				return fallbackResponse, false, nil
			}

			return nil, false, errors.Join(errs...)

		case <-time.After(cfg.pollInterval):
		}
	}
}

// decodeInto unmarshals raw into resp, tolerating a nil resp or an empty body.
func decodeInto(raw json.RawMessage, resp any) error {
	if resp == nil || len(raw) == 0 {
		return nil
	}

	if err := json.Unmarshal(raw, resp); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	return nil
}

// broadcastWrite runs fn against every handler concurrently and returns the
// result of the first handler to succeed. If every handler fails, the joined
// error is returned.
func broadcastWrite[T any](ctx context.Context, handlers []*handler, fn func(context.Context, *handler) (T, error)) (T, error) {
	bgCtx := context.WithoutCancel(ctx)
	cancel := func() {}
	if deadline, ok := ctx.Deadline(); ok {
		bgCtx, cancel = context.WithDeadline(bgCtx, deadline)
	}

	type result struct {
		val T
		err error
	}

	var wg sync.WaitGroup
	results := make(chan result, len(handlers))
	for _, h := range handlers {
		wg.Go(func() {
			val, err := fn(bgCtx, h)
			results <- result{val: val, err: err}
		})
	}

	// Release the deadline timer once every detached write has finished.
	go func() {
		wg.Wait()
		cancel()
	}()

	var errs []error
	for range handlers {
		select {
		case r := <-results:
			if r.err == nil {
				return r.val, nil
			}

			errs = append(errs, r.err)
		case <-ctx.Done():
			for {
				select {
				case r := <-results:
					if r.err == nil {
						return r.val, nil
					}

					errs = append(errs, r.err)
				default:
					var zero T
					return zero, ctx.Err()
				}
			}
		}
	}

	var zero T
	return zero, errors.Join(errs...)
}

// broadcastWriteAll runs fn against every handler concurrently (on a context
// detached from caller cancellation, like broadcastWrite) and waits for all of
// them, returning the successful values and the errors separately.
func broadcastWriteAll[T any](ctx context.Context, handlers []*handler, fn func(context.Context, *handler) (T, error)) ([]T, []error) {
	bgCtx := context.WithoutCancel(ctx)
	cancel := func() {}
	if deadline, ok := ctx.Deadline(); ok {
		bgCtx, cancel = context.WithDeadline(bgCtx, deadline)
	}

	type result struct {
		val T
		err error
	}

	var wg sync.WaitGroup
	results := make(chan result, len(handlers))
	for _, h := range handlers {
		wg.Go(func() {
			val, err := fn(bgCtx, h)
			results <- result{val: val, err: err}
		})
	}

	// Release the deadline timer once every detached write has finished.
	go func() {
		wg.Wait()
		cancel()
	}()

	var (
		vals []T
		errs []error
	)

	collect := func(r result) {
		if r.err != nil {
			errs = append(errs, r.err)
			return
		}

		vals = append(vals, r.val)
	}

	for range handlers {
		select {
		case r := <-results:
			collect(r)
		case <-ctx.Done():
			for {
				select {
				case r := <-results:
					collect(r)
				default:
					return vals, errs
				}
			}
		}
	}

	return vals, errs
}

// cloneBuffer returns a fresh buffer over a copy of raw, or nil when the
// original data was nil.
func cloneBuffer(data *bytes.Buffer, raw []byte) *bytes.Buffer {
	if data == nil {
		return nil
	}

	return bytes.NewBuffer(bytes.Clone(raw))
}
