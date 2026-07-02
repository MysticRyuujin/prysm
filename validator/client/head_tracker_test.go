package client

import (
	"sync"
	"testing"

	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/testing/assert"
)

func TestHeadTracker_EmptyUntilFirstUpdate(t *testing.T) {
	h := newHeadTracker()
	_, _, ok := h.latest()
	assert.Equal(t, false, ok)
}

func TestHeadTracker_NewestWins(t *testing.T) {
	h := newHeadTracker()
	h.update(10, [32]byte{0xaa})
	h.update(11, [32]byte{0xbb})

	root, slot, ok := h.latest()
	assert.Equal(t, true, ok)
	assert.Equal(t, primitives.Slot(11), slot)
	assert.Equal(t, byte(0xbb), root[0])
}

func TestHeadTracker_OlderSlotIgnored(t *testing.T) {
	h := newHeadTracker()
	h.update(11, [32]byte{0xbb})
	h.update(10, [32]byte{0xaa}) // older, must be ignored

	root, slot, _ := h.latest()
	assert.Equal(t, primitives.Slot(11), slot)
	assert.Equal(t, byte(0xbb), root[0])
}

func TestHeadTracker_SameSlotReorgReplaces(t *testing.T) {
	h := newHeadTracker()
	h.update(11, [32]byte{0xbb})
	h.update(11, [32]byte{0xcc}) // same slot, different root wins

	root, _, _ := h.latest()
	assert.Equal(t, byte(0xcc), root[0])
}

func TestHeadTracker_ConcurrentAccess(t *testing.T) {
	h := newHeadTracker()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) { defer wg.Done(); h.update(primitives.Slot(i), [32]byte{byte(i)}) }(i)
		go func() { defer wg.Done(); _, _, _ = h.latest() }()
	}
	wg.Wait()
}
