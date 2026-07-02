package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	"github.com/OffchainLabs/prysm/v7/validator/client/iface"
)

type headTracker struct {
	mu sync.RWMutex

	slot primitives.Slot
	root [32]byte
	set  bool
}

func newHeadTracker() *headTracker {
	return &headTracker{}
}

func (h *headTracker) update(slot primitives.Slot, root [32]byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.set && slot < h.slot {
		return
	}

	h.slot, h.root, h.set = slot, root, true
}

func (h *headTracker) latest() ([32]byte, primitives.Slot, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.root, h.slot, h.set
}

func (v *validator) recordHeadRoot(slot primitives.Slot, blockRoot string) error {
	root, err := bytesutil.DecodeHex32(blockRoot)
	if err != nil {
		return fmt.Errorf("decode event block root: %w", err)
	}

	v.head.update(slot, root)

	return nil
}

func (v *validator) withHeadHint(ctx context.Context, slot primitives.Slot, component primitives.BP) context.Context {
	return v.withHint(ctx, slot, component, v.head.latest)
}

func (v *validator) withPayloadHeadHint(ctx context.Context, slot primitives.Slot) context.Context {
	head := func() ([32]byte, primitives.Slot, bool) {
		root, ok := v.payloadAvailability.payloadRoot(slot)
		return root, slot, ok
	}

	return v.withHint(ctx, slot, params.BeaconConfig().PayloadAttestationDueBPS, head)
}

func (v *validator) withHint(
	ctx context.Context,
	slot primitives.Slot,
	component primitives.BP,
	head func() (root [32]byte, slot primitives.Slot, ok bool),
) context.Context {
	hint := iface.Hint{Head: head}
	if deadline, err := v.slotComponentDeadline(slot, component); err == nil {
		hint.Deadline = deadline
	}

	return iface.WithHint(ctx, hint)
}
