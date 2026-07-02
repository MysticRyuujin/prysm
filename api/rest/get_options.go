package rest

import (
	"encoding/json"
	"net/http"
	"time"
)

const defaultPollInterval = 50 * time.Millisecond // backoff between re-polling

// getConfig holds the per-call configuration assembled from GetOption values.
type getConfig struct {
	accept       func(json.RawMessage) bool              // when non-nil, turns a JSON Get into a race until match. When nil, Get defaults to first success behavior.
	sszAccept    func(body []byte, hdr http.Header) bool // when non-nil, turns a GetSSZ into a race that prefers a body satisfying sszAccept. When nil, GetSSZ defaults to first success behavior.
	pollInterval time.Duration                           // backoff between re-polling rounds. Zero uses defaultPollInterval.
	deadline     time.Time                               // bounds a race-until-match read (the single round, and re-polling when enabled). Zero means unbounded.
	repoll       bool                                    // when true, keep re-polling all nodes until deadline; when false, do a single bounded round.
}

// GetOption customizes a Handler.Get call.
type GetOption func(*getConfig)

// newGetConfig folds opts into a getConfig.
func newGetConfig(opts []GetOption) getConfig {
	var cfg getConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.pollInterval <= 0 {
		cfg.pollInterval = defaultPollInterval
	}

	return cfg
}

// WithAccept makes the read keep polling all beacon nodes until one returns a
// 2XX body that satisfies accept, or the deadline fires.
func WithAccept(accept func(raw json.RawMessage) bool) GetOption {
	return func(c *getConfig) {
		c.accept = accept
	}
}

// WithSSZAccept makes GetSSZ prefer a node whose (2XX) response satisfies
// accept, racing all nodes and returning the first match. If no node matches
// before the deadline (see WithDeadline), it falls back to the first successful
// response.
func WithSSZAccept(accept func(body []byte, hdr http.Header) bool) GetOption {
	return func(c *getConfig) {
		c.sszAccept = accept
	}
}

// WithPollInterval sets the backoff between re-polling rounds.
func WithPollInterval(d time.Duration) GetOption {
	return func(c *getConfig) {
		c.pollInterval = d
	}
}

// WithDeadline bounds how long a race-until-match read runs: it caps the single
// round and, when WithRepoll is set, how long re-polling continues.
func WithDeadline(t time.Time) GetOption {
	return func(c *getConfig) {
		c.deadline = t
	}
}

// WithRepoll makes a race-until-match read keep re-polling all nodes (every
// poll interval) until a node matches or the deadline fires.
func WithRepoll() GetOption {
	return func(c *getConfig) {
		c.repoll = true
	}
}
