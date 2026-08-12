package eth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/p2p"
	"github.com/OffchainLabs/prysm/v7/consensus-types/blocks"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	ethtypes "github.com/OffchainLabs/prysm/v7/proto/prysm/v1alpha1"
	"github.com/OffchainLabs/prysm/v7/time/slots"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	ssz "github.com/prysmaticlabs/fastssz"

	"github.com/probe-lab/hermes/tele"
)

// ValidationMode selects how much of a gossip message hermes checks before it
// forwards it on.
type ValidationMode string

const (
	// ValidationModeOff keeps upstream behaviour: everything is forwarded, and
	// only decoded afterwards for observation.
	ValidationModeOff ValidationMode = "off"
	// ValidationModeStructural runs only the checks needing no external data.
	ValidationModeStructural ValidationMode = "structural"
)

// defaultSlotWindow is how far from the current slot a block may claim to be
// before it is ignored. Wide enough to absorb clock skew and a short reorg.
const defaultSlotWindow = 4

// ValidationConfig configures the gossip validator.
type ValidationConfig struct {
	Mode       ValidationMode
	SlotWindow uint64
}

func (v ValidationConfig) Validate() error {
	switch v.Mode {
	// The empty string is the zero value of the struct, so it means "unset", which
	// must behave as off rather than failing a node that never opted in.
	case "", ValidationModeOff, ValidationModeStructural:
		return nil
	default:
		return fmt.Errorf("invalid validation mode %q, want off or structural", v.Mode)
	}
}

func (v ValidationConfig) enabled() bool {
	return v.Mode == ValidationModeStructural
}

func (v ValidationConfig) slotWindow() uint64 {
	if v.SlotWindow == 0 {
		return defaultSlotWindow
	}
	return v.SlotWindow
}

// Outcome reasons, used as metric labels and on the emitted trace event.
const (
	reasonDecode       = "decode"
	reasonUnknownFork  = "unknown_fork"
	reasonSlotWindow   = "slot_out_of_window"
	reasonEquivocation = "equivocation"
)

// blockForFork returns an empty signed block of the type matching fork.
func blockForFork(fork chainFork) (ssz.Unmarshaler, error) {
	switch fork {
	case phase0:
		return &ethtypes.SignedBeaconBlock{}, nil
	case altair:
		return &ethtypes.SignedBeaconBlockAltair{}, nil
	case bellatrix:
		return &ethtypes.SignedBeaconBlockBellatrix{}, nil
	case capella:
		return &ethtypes.SignedBeaconBlockCapella{}, nil
	case deneb:
		return &ethtypes.SignedBeaconBlockDeneb{}, nil
	case electra:
		return &ethtypes.SignedBeaconBlockElectra{}, nil
	case fulu:
		return &ethtypes.SignedBeaconBlockFulu{}, nil
	default:
		return nil, fmt.Errorf("unrecognized fork-version: %d", fork)
	}
}

// validateBeaconBlock is the gossipsub topic validator for beacon_block.
//
// Performs no I/O: the schedule comes from the chain's epoch loop, and a miss
// degrades to the structural checks rather than fetching.
//
// REJECT debits the sender's score, so it is reserved for provably bad messages;
// anything merely unconfirmable returns IGNORE.
func (p *PubSub) validateBeaconBlock(ctx context.Context, _ peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	start := time.Now()
	topic := msg.GetTopic()

	block, err := blockForFork(p.cfg.Chain.CurrentFork())
	if err != nil {
		slog.Warn("cannot validate block for unknown fork", "topic", topic, tele.LogAttrError(err))
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonUnknownFork, err, start)
	}

	if err := p.cfg.Encoder.DecodeGossip(msg.Data, block); err != nil {
		return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonDecode, err, start)
	}

	wrapped, err := blocks.NewSignedBeaconBlock(block)
	if err != nil {
		return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonDecode, err, start)
	}
	if wrapped.IsNil() || wrapped.Block().IsNil() {
		return p.finishValidation(ctx, msg, pubsub.ValidationReject, reasonDecode, errors.New("nil block"), start)
	}

	slot := wrapped.Block().Slot()
	currentSlot := slots.CurrentSlot(p.cfg.Chain.cfg.GenesisConfig.GenesisTime)
	if slotDistance(slot, currentSlot) > p.cfg.Validation.slotWindow() {
		return p.finishValidation(ctx, msg, pubsub.ValidationIgnore, reasonSlotWindow, nil, start)
	}

	// Hand the decoded block to the subscription handler so the accept path
	// decodes exactly once.
	msg.ValidatorData = block

	return p.finishValidation(ctx, msg, pubsub.ValidationAccept, "", nil, start)
}

func slotDistance(a, b primitives.Slot) uint64 {
	if a > b {
		return uint64(a - b)
	}
	return uint64(b - a)
}

// mapPubSubTopicWithValidators returns the validator for a topic, or nil to keep
// upstream's accept-everything behaviour.
func (p *PubSub) mapPubSubTopicWithValidators(topic string) pubsub.ValidatorEx {
	if !p.cfg.Validation.enabled() {
		return nil
	}

	if strings.Contains(topic, p2p.GossipBlockMessage) {
		return p.validateBeaconBlock
	}
	return nil
}
