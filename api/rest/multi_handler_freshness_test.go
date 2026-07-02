package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OffchainLabs/prysm/v7/api"
	"github.com/OffchainLabs/prysm/v7/testing/assert"
	"github.com/OffchainLabs/prysm/v7/testing/require"
)

type rootResponse struct {
	Root string `json:"root"`
}

// rootServer serves {"root": rootFn()} after an optional delay, counting hits.
func rootServer(t *testing.T, delay time.Duration, rootFn func() string, hits *int32) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits != nil {
			atomic.AddInt32(hits, 1)
		}
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", api.JsonMediaType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"root":"` + rootFn() + `"}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func acceptRoot(want string) func(json.RawMessage) bool {
	return func(raw json.RawMessage) bool {
		var r rootResponse
		if err := json.Unmarshal(raw, &r); err != nil {
			return false
		}
		return r.Root == want
	}
}

func constRoot(s string) func() string { return func() string { return s } }

// A fast-but-stale node must not win over a slower node that has the fresh head.
func TestMultiHandler_Get_Accept_FreshWinsOverFastStale(t *testing.T) {
	stale := rootServer(t, 0, constRoot("stale"), nil)
	fresh := rootServer(t, 30*time.Millisecond, constRoot("fresh"), nil)

	mh := multi(t, stale.URL, fresh.URL)
	var resp rootResponse
	err := mh.Get(context.Background(), "/x", &resp,
		WithAccept(acceptRoot("fresh")),
		WithPollInterval(10*time.Millisecond),
		WithDeadline(time.Now().Add(2*time.Second)),
	)
	require.NoError(t, err)
	assert.Equal(t, "fresh", resp.Root)
}

// When no node ever matches, the freshest successful body is returned as a
// best-effort fallback (no error).
func TestMultiHandler_Get_Accept_BestAvailableOnDeadline(t *testing.T) {
	stale1 := rootServer(t, 0, constRoot("stale"), nil)
	stale2 := rootServer(t, 0, constRoot("stale"), nil)

	mh := multi(t, stale1.URL, stale2.URL)
	var resp rootResponse
	err := mh.Get(context.Background(), "/x", &resp,
		WithAccept(acceptRoot("fresh")),
		WithPollInterval(20*time.Millisecond),
		WithDeadline(time.Now().Add(100*time.Millisecond)),
	)
	require.NoError(t, err)
	assert.Equal(t, "stale", resp.Root, "should fall back to the freshest available body")
}

// If every node errors, the accept path returns an error.
func TestMultiHandler_Get_Accept_AllFail(t *testing.T) {
	bad1 := jsonServer(t, 0, http.StatusInternalServerError, nil)
	bad2 := jsonServer(t, 0, http.StatusBadGateway, nil)

	mh := multi(t, bad1.URL, bad2.URL)
	var resp rootResponse
	err := mh.Get(context.Background(), "/x", &resp,
		WithAccept(acceptRoot("fresh")),
		WithDeadline(time.Now().Add(100*time.Millisecond)),
	)
	require.NotNil(t, err)
}

// A single lagging node is re-polled until it imports the announced head.
func TestMultiHandler_Get_Accept_SingleNodeCatchesUp(t *testing.T) {
	var hits int32
	rootFn := func() string {
		if atomic.LoadInt32(&hits) >= 3 {
			return "fresh"
		}
		return "stale"
	}
	srv := rootServer(t, 0, rootFn, &hits)

	mh := multi(t, srv.URL) // single handler still re-polls via the accept path
	var resp rootResponse
	err := mh.Get(context.Background(), "/x", &resp,
		WithAccept(acceptRoot("fresh")),
		WithPollInterval(5*time.Millisecond),
		WithDeadline(time.Now().Add(2*time.Second)),
		WithRepoll(),
	)
	require.NoError(t, err)
	assert.Equal(t, "fresh", resp.Root)
	assert.Equal(t, true, atomic.LoadInt32(&hits) >= 3, "should have re-polled the lagging node across multiple rounds")
}

// raceReadUntil returns matched=true and stops as soon as a response satisfies
// the predicate.
func TestRaceReadUntil_ReturnsFirstMatch(t *testing.T) {
	h1 := newTestHandler("http://h1")
	h2 := newTestHandler("http://h2")
	fn := func(_ context.Context, h *handler) (json.RawMessage, error) {
		if h == h2 {
			return json.RawMessage(`{"root":"fresh"}`), nil
		}
		return json.RawMessage(`{"root":"stale"}`), nil
	}
	cfg := getConfig{accept: acceptRoot("fresh"), pollInterval: time.Millisecond, deadline: time.Now().Add(time.Second)}
	raw, matched, err := raceReadUntil(context.Background(), []*handler{h1, h2}, cfg, fn)
	require.NoError(t, err)
	assert.Equal(t, true, matched)
	var r rootResponse
	require.NoError(t, json.Unmarshal(raw, &r))
	assert.Equal(t, "fresh", r.Root)
}

// With the deadline already passed, exactly one best-effort round runs and the
// freshest successful body is returned with matched=false.
func TestRaceReadUntil_DeadlinePassed_BestEffortSingleRound(t *testing.T) {
	var rounds int32
	h1 := newTestHandler("http://h1")
	fn := func(_ context.Context, _ *handler) (json.RawMessage, error) {
		atomic.AddInt32(&rounds, 1)
		return json.RawMessage(`{"root":"stale"}`), nil
	}
	cfg := getConfig{accept: acceptRoot("fresh"), pollInterval: time.Second, deadline: time.Now().Add(-time.Second)}
	raw, matched, err := raceReadUntil(context.Background(), []*handler{h1}, cfg, fn)
	require.NoError(t, err)
	assert.Equal(t, false, matched)
	assert.Equal(t, int32(1), atomic.LoadInt32(&rounds), "deadline in the past should run exactly one round")
	var r rootResponse
	require.NoError(t, json.Unmarshal(raw, &r))
	assert.Equal(t, "stale", r.Root)
}

// If every handler errors and nothing matches, the joined error is returned.
func TestRaceReadUntil_AllError(t *testing.T) {
	h1 := newTestHandler("http://h1")
	sentinel := errors.New("boom")
	fn := func(_ context.Context, _ *handler) (json.RawMessage, error) {
		return nil, sentinel
	}
	cfg := getConfig{accept: acceptRoot("fresh"), pollInterval: time.Millisecond, deadline: time.Now().Add(-time.Second)}
	_, matched, err := raceReadUntil(context.Background(), []*handler{h1}, cfg, fn)
	require.NotNil(t, err)
	assert.Equal(t, false, matched)
	assert.Equal(t, true, errors.Is(err, sentinel))
}

// A cancelled context returns promptly (never matched). With a realistic fn
// that honors ctx, no success is recorded, so the context error surfaces.
func TestRaceReadUntil_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h1 := newTestHandler("http://h1")
	fn := func(ctx context.Context, _ *handler) (json.RawMessage, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return json.RawMessage(`{"root":"stale"}`), nil
	}
	cfg := getConfig{accept: acceptRoot("fresh"), pollInterval: time.Second, deadline: time.Now().Add(time.Hour)}
	_, matched, err := raceReadUntil(ctx, []*handler{h1}, cfg, fn)
	assert.Equal(t, false, matched)
	assert.Equal(t, true, errors.Is(err, context.Canceled))
}

// If a best-available response was recorded before cancellation, it is returned
// (rather than the context error) when the caller later cancels.
func TestRaceReadUntil_ContextCancelReturnsBestAvailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h1 := newTestHandler("http://h1")
	var calls int32
	fn := func(_ context.Context, _ *handler) (json.RawMessage, error) {
		// First round yields a stale (best-available) response; cancel so the
		// next round's drain observes ctx.Done() with a best-available in hand.
		if atomic.AddInt32(&calls, 1) == 1 {
			return json.RawMessage(`{"root":"stale"}`), nil
		}
		cancel()
		return json.RawMessage(`{"root":"stale"}`), nil
	}
	cfg := getConfig{accept: acceptRoot("fresh"), pollInterval: time.Millisecond, deadline: time.Now().Add(time.Hour), repoll: true}
	raw, matched, err := raceReadUntil(ctx, []*handler{h1}, cfg, fn)
	require.NoError(t, err)
	assert.Equal(t, false, matched)
	var r rootResponse
	require.NoError(t, json.Unmarshal(raw, &r))
	assert.Equal(t, "stale", r.Root)
}

// raceReadAccept returns the first result satisfying accept.
func TestRaceReadAccept_ReturnsFirstMatch(t *testing.T) {
	h1 := newTestHandler("http://h1")
	h2 := newTestHandler("http://h2")
	fn := func(_ context.Context, h *handler) (json.RawMessage, error) {
		if h == h2 {
			return json.RawMessage(`{"root":"fresh"}`), nil
		}
		return json.RawMessage(`{"root":"stale"}`), nil
	}
	raw, err := raceReadAccept(context.Background(), []*handler{h1, h2}, time.Now().Add(time.Second), acceptRoot("fresh"), fn)
	require.NoError(t, err)
	var r rootResponse
	require.NoError(t, json.Unmarshal(raw, &r))
	assert.Equal(t, "fresh", r.Root)
}

// When no node matches, raceReadAccept falls back to a successful response.
func TestRaceReadAccept_FallsBackWhenNoMatch(t *testing.T) {
	h1 := newTestHandler("http://h1")
	fn := func(_ context.Context, _ *handler) (json.RawMessage, error) {
		return json.RawMessage(`{"root":"stale"}`), nil
	}
	raw, err := raceReadAccept(context.Background(), []*handler{h1}, time.Now().Add(50*time.Millisecond), acceptRoot("fresh"), fn)
	require.NoError(t, err)
	var r rootResponse
	require.NoError(t, json.Unmarshal(raw, &r))
	assert.Equal(t, "stale", r.Root)
}

// A fast-but-stale node lets raceReadAccept fall back at the deadline without
// waiting for a hung node.
func TestRaceReadAccept_DoesNotBlockOnHungHandler(t *testing.T) {
	fast := newTestHandler("http://fast")
	hung := newTestHandler("http://hung")
	fn := func(ctx context.Context, h *handler) (json.RawMessage, error) {
		if h == hung {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return json.RawMessage(`{"root":"stale"}`), nil
	}
	start := time.Now()
	raw, err := raceReadAccept(context.Background(), []*handler{fast, hung}, time.Now().Add(80*time.Millisecond), acceptRoot("fresh"), fn)
	require.NoError(t, err)
	assert.Equal(t, true, time.Since(start) < time.Second, "must not wait on the hung handler")
	var r rootResponse
	require.NoError(t, json.Unmarshal(raw, &r))
	assert.Equal(t, "stale", r.Root)
}

// If every handler errors, raceReadAccept returns the joined error.
func TestRaceReadAccept_AllFail(t *testing.T) {
	h1 := newTestHandler("http://h1")
	sentinel := errors.New("boom")
	fn := func(_ context.Context, _ *handler) (json.RawMessage, error) {
		return nil, sentinel
	}
	_, err := raceReadAccept(context.Background(), []*handler{h1}, time.Now().Add(time.Second), acceptRoot("fresh"), fn)
	require.NotNil(t, err)
	assert.Equal(t, true, errors.Is(err, sentinel))
}

// sszServer serves body as an octet-stream 200.
func sszServer(t *testing.T, body string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", api.OctetStreamMediaType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// GetSSZ with WithSSZAccept prefers the node whose body satisfies the predicate.
func TestMultiHandler_GetSSZ_Accept_PrefersMatch(t *testing.T) {
	stale := sszServer(t, "stale")
	fresh := sszServer(t, "fresh")

	mh := multi(t, stale.URL, fresh.URL)
	body, _, err := mh.GetSSZ(context.Background(), "/x",
		WithSSZAccept(func(b []byte, _ http.Header) bool { return string(b) == "fresh" }),
		WithDeadline(time.Now().Add(2*time.Second)),
	)
	require.NoError(t, err)
	assert.Equal(t, "fresh", string(body))
}

// GetSSZ with WithSSZAccept falls back to a successful body when none match.
func TestMultiHandler_GetSSZ_Accept_FallsBackWhenNoMatch(t *testing.T) {
	a := sszServer(t, "stale")
	b := sszServer(t, "stale")

	mh := multi(t, a.URL, b.URL)
	body, _, err := mh.GetSSZ(context.Background(), "/x",
		WithSSZAccept(func(bd []byte, _ http.Header) bool { return string(bd) == "fresh" }),
		WithDeadline(time.Now().Add(100*time.Millisecond)),
	)
	require.NoError(t, err)
	assert.Equal(t, "stale", string(body))
}
