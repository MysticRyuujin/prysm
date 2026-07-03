package beacon_api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/OffchainLabs/prysm/v7/api/rest"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/encoding/bytesutil"
	ethpb "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/validator/client/iface"
)

const (
	blockFreshnessBudget = 500 * time.Millisecond
	readFreshnessBudget  = 500 * time.Millisecond
)

type headExtractor struct {
	extract func(raw json.RawMessage) (root [32]byte, ok bool)
	poll    bool // keeps re-polling all nodes until the hint deadlin
}

var (
	attestationRootExtractor = headExtractor{extract: rootExtractor("beacon_block_root"), poll: true}
	syncBlockRootExtractor   = headExtractor{extract: rootExtractor("root"), poll: false}
)

// freshnessHint returns the freshness hint on ctx, if one usable for head
// matching is present.
func freshnessHint(ctx context.Context) (iface.Hint, bool) {
	hint, ok := iface.FromContext(ctx)
	if !ok || hint.Head == nil {
		return iface.Hint{}, false
	}

	return hint, true
}

func freshnessOptions(ctx context.Context, extractor headExtractor) []rest.GetOption {
	hint, ok := freshnessHint(ctx)
	if !ok {
		return nil
	}

	accept := func(raw json.RawMessage) bool {
		wantRoot, _, known := hint.Head()
		if !known {
			// No head expectation yet: we cannot do better than first-success.
			return true
		}

		gotRoot, ok := extractor.extract(raw)
		return ok && gotRoot == wantRoot
	}

	opts := []rest.GetOption{rest.WithAccept(accept)}
	if hint.Deadline.IsZero() {
		return opts
	}

	deadline := hint.Deadline
	if floor := time.Now().Add(readFreshnessBudget); deadline.Before(floor) {
		deadline = floor
	}

	opts = append(opts, rest.WithDeadline(deadline))
	if extractor.poll {
		opts = append(opts, rest.WithRepoll())
	}

	return opts
}

func blockFreshnessOptions(ctx context.Context, decode func([]byte, http.Header) (*ethpb.GenericBeaconBlock, error)) []rest.GetOption {
	hint, ok := freshnessHint(ctx)
	if !ok {
		return nil
	}

	accept := func(body []byte, hdr http.Header) bool {
		wantRoot, _, known := hint.Head()
		if !known {
			// No head expectation yet: we cannot do better than first-success.
			return true
		}

		block, err := decode(body, hdr)
		if err != nil {
			return false
		}

		wrapped, err := blocks.NewBeaconBlock(block.Block)
		if err != nil {
			return false
		}

		return wrapped.ParentRoot() == wantRoot
	}

	deadline := time.Now().Add(blockFreshnessBudget)
	if !hint.Deadline.IsZero() && hint.Deadline.Before(deadline) {
		deadline = hint.Deadline
	}

	return []rest.GetOption{rest.WithSSZAccept(accept), rest.WithDeadline(deadline)}
}

// rootExtractor returns an extractor that reads a 32-byte hex root from
// data.<field> of a JSON response.
func rootExtractor(field string) func(json.RawMessage) ([32]byte, bool) {
	return func(raw json.RawMessage) ([32]byte, bool) {
		var body struct {
			Data map[string]json.RawMessage `json:"data"`
		}

		if err := json.Unmarshal(raw, &body); err != nil {
			return [32]byte{}, false
		}

		var hexRoot string
		if err := json.Unmarshal(body.Data[field], &hexRoot); err != nil || hexRoot == "" {
			return [32]byte{}, false
		}

		root, err := bytesutil.DecodeHex32(hexRoot)
		if err != nil {
			return [32]byte{}, false
		}

		return root, true
	}
}
