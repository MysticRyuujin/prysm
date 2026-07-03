package event

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	lruwrpr "github.com/OffchainLabs/prysm/v7/cache/lru"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	initialReconnectBackoff  = 1 * time.Second         // wait before the first reconnect attempt to a host.
	maxReconnectBackoff      = 16 * time.Second        // caps the per-host reconnect backoff.
	healthyReconnectDuration = 5 * time.Second         // a subscription that stays up at least this long is considered healthy and resets the backoff.
	dedupCapacity            = 256                     // bounds the set of recently-seen events used to drop duplicates emitted by multiple beacon nodes.
	outageSilence            = 2 * maxReconnectBackoff // how long the merged feed may go without a single real event (while stream errors are occurring) before it is treated as a total outage.
)

type MultiEventStream struct {
	ctx        context.Context
	httpClient *http.Client
	hosts      []string
	topics     []string
}

var _ EventStreamClient = &MultiEventStream{}

// NewMultiEventStream creates a MultiEventStream over the given hosts.
func NewMultiEventStream(ctx context.Context, httpClient *http.Client, hosts []string, topics []string) (*MultiEventStream, error) {
	if len(hosts) == 0 {
		return nil, errors.New("no hosts provided")
	}

	if len(topics) == 0 {
		return nil, errors.New("no topics provided")
	}

	multiEventStream := &MultiEventStream{
		ctx:        ctx,
		httpClient: httpClient,
		hosts:      hosts,
		topics:     topics,
	}

	return multiEventStream, nil
}

// Subscribe runs one reconnecting SSE subscription per host, merges them, drops
// duplicates, and forwards events to out. It blocks until the context is
// cancelled.
func (m *MultiEventStream) Subscribe(out chan<- *Event) {
	merged := make(chan *Event, len(m.hosts))

	var wg sync.WaitGroup
	for _, host := range m.hosts {
		wg.Go(func() {
			m.runHost(host, merged)
		})
	}

	go func() {
		wg.Wait()
		close(merged)
	}()

	deduper := newDeduper(dedupCapacity)

	errSinceEvent := false
	feedWasAlive := false
	watchdog := time.NewTimer(outageSilence)
	defer watchdog.Stop()

	for {
		select {
		case event, ok := <-merged:
			if !ok {
				return
			}

			if event.Type == EventError || event.Type == EventConnectionError {
				log.WithField("data", string(event.Data)).Warning("Beacon node event stream reported an error")
				errSinceEvent = true
				continue
			}

			if deduper.seen(event) {
				continue
			}

			if !m.send(out, event) {
				return
			}

			// A real event reached the validator: the feed is alive
			// Re-arm the watchdog.
			errSinceEvent = false
			feedWasAlive = true
			if !watchdog.Stop() {
				select {
				case <-watchdog.C:
				default:
				}
			}
			watchdog.Reset(outageSilence)
		case <-watchdog.C:
			watchdog.Reset(outageSilence)
			if !errSinceEvent && !feedWasAlive {
				continue
			}

			errSinceEvent = false
			if !m.send(out, &Event{
				Type: EventConnectionError,
				Data: []byte("no events received from any beacon node event stream"),
			}) {
				return
			}
		case <-m.ctx.Done():
			return
		}
	}
}

// send forwards e to out, returning false if the context is cancelled first.
func (m *MultiEventStream) send(out chan<- *Event, e *Event) bool {
	select {
	case out <- e:
		return true
	case <-m.ctx.Done():
		return false
	}
}

// runHost maintains a single host's subscription, reconnecting with capped
// exponential backoff until the context is cancelled.
func (m *MultiEventStream) runHost(host string, merged chan<- *Event) {
	backoff := initialReconnectBackoff
	for {
		if m.ctx.Err() != nil {
			return
		}

		stream, err := NewEventStream(m.ctx, m.httpClient, host, m.topics)
		if err != nil {
			select {
			case merged <- &Event{
				Type: EventConnectionError,
				Data: []byte(errors.Wrapf(err, "invalid event stream host %s", host).Error()),
			}:
			case <-m.ctx.Done():
			}
			return
		}

		start := time.Now()

		// Blocking call
		stream.Subscribe(merged)

		if m.ctx.Err() != nil {
			return
		}

		if time.Since(start) >= healthyReconnectDuration {
			backoff = initialReconnectBackoff
		}

		log.WithFields(logrus.Fields{"host": host, "backoff": backoff}).Debug("Beacon node event stream disconnected, reconnecting")

		select {
		case <-time.After(backoff):
		case <-m.ctx.Done():
			return
		}

		if backoff *= 2; backoff > maxReconnectBackoff {
			backoff = maxReconnectBackoff
		}
	}
}

type deduper struct {
	cache *lru.Cache
}

func newDeduper(capacity int) *deduper {
	return &deduper{cache: lruwrpr.New(capacity)}
}

// seen reports whether e was already seen, recording it as seen otherwise.
func (d *deduper) seen(e *Event) bool {
	key := e.Type + "\x00" + canonicalPayload(e.Data)
	existed, _ := d.cache.ContainsOrAdd(key, nil)
	return existed
}

// canonicalPayload normalizes an event's JSON payload so that a single logical
// event emitted by multiple beacon nodes produces the same dedup key even when
// the nodes (often different client implementations) order object fields or
// insert whitespace differently. Non-JSON payloads are returned unchanged.
//
// Note this does not reconcile fields that legitimately differ between nodes
// for the same head (e.g. execution_optimistic); such events remain distinct.
func canonicalPayload(data []byte) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return string(data)
	}

	// json.Marshal emits map keys in sorted order, giving a stable encoding
	// independent of the source node's field ordering and whitespace.
	canonical, err := json.Marshal(fields)
	if err != nil {
		return string(data)
	}

	return string(canonical)
}
