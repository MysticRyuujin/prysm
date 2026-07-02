package client

import (
	"sync"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
)

// payloadAvailability releases per-slot waiters when an execution_payload_available
// event is received, so PTC members can attest as soon as the payload and blobs are
// available rather than waiting for the attestation deadline.
type payloadAvailability struct {
	mu    sync.Mutex
	chans map[primitives.Slot]chan struct{}
	roots map[primitives.Slot][32]byte
}

func newPayloadAvailability() *payloadAvailability {
	return &payloadAvailability{
		chans: make(map[primitives.Slot]chan struct{}),
		roots: make(map[primitives.Slot][32]byte),
	}
}

// waiter returns a channel closed once the payload for slot is available.
func (p *payloadAvailability) waiter(slot primitives.Slot) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch, ok := p.chans[slot]
	if !ok {
		ch = make(chan struct{})
		p.chans[slot] = ch
	}
	return ch
}

// notify releases waiters for slot, records the announced payload block root
// (for freshness matching) when one is provided, and prunes older slots.
func (p *payloadAvailability) notify(slot primitives.Slot, root *[32]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if root != nil {
		p.roots[slot] = *root
	}
	ch, ok := p.chans[slot]
	if !ok {
		ch = make(chan struct{})
		p.chans[slot] = ch
	}
	select {
	case <-ch:
	default:
		close(ch)
	}

	for s := range p.chans {
		if s < slot {
			delete(p.chans, s)
		}
	}

	for s := range p.roots {
		if s < slot {
			delete(p.roots, s)
		}
	}
}

// payloadRoot returns the payload block root announced for slot, if any.
func (p *payloadAvailability) payloadRoot(slot primitives.Slot) ([32]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	r, ok := p.roots[slot]
	return r, ok
}
