package eth

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/OffchainLabs/prysm/v7/beacon-chain/core/signing"
	"github.com/OffchainLabs/prysm/v7/config/params"
	"github.com/OffchainLabs/prysm/v7/consensus-types/primitives"
	"github.com/OffchainLabs/prysm/v7/time/slots"

	"github.com/probe-lab/hermes/tele"
)

// epochDuties is the proposer schedule for one epoch plus its signing domain.
// The domain costs a hash to derive and only changes per epoch, so it is
// computed here rather than per message.
type epochDuties struct {
	duties map[primitives.Slot]ProposerDuty
	domain []byte
}

// proposerDutyCache serves the proposer schedule to gossip validation without
// I/O. It is filled on the chain's epoch loop and only ever read on the message
// path: a beacon node may be remote and shared by several hermes instances, so a
// per-message fetch would multiply load on it and add its round trip to
// propagation.
type proposerDutyCache struct {
	mu     sync.RWMutex
	epochs map[primitives.Epoch]*epochDuties
}

func newProposerDutyCache() *proposerDutyCache {
	return &proposerDutyCache{epochs: make(map[primitives.Epoch]*epochDuties, 2)}
}

// Lookup returns the duty and signing domain for slot. The bool is false when
// the epoch has not been fetched, which callers must treat as "cannot verify"
// rather than "invalid".
func (c *proposerDutyCache) Lookup(slot primitives.Slot) (ProposerDuty, []byte, bool) {
	epoch := slots.ToEpoch(slot)

	c.mu.RLock()
	defer c.mu.RUnlock()

	ed, ok := c.epochs[epoch]
	if !ok {
		return ProposerDuty{}, nil, false
	}

	duty, ok := ed.duties[slot]
	if !ok {
		return ProposerDuty{}, nil, false
	}

	return duty, ed.domain, true
}

func (c *proposerDutyCache) has(epoch primitives.Epoch) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.epochs[epoch]
	return ok
}

// store inserts the epoch and drops any epoch outside keep, bounding the cache
// to the epochs the slot window can still reach.
func (c *proposerDutyCache) store(epoch primitives.Epoch, ed *epochDuties, keep []primitives.Epoch) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.epochs[epoch] = ed

	for cached := range c.epochs {
		if !containsEpoch(keep, cached) {
			delete(c.epochs, cached)
		}
	}
}

func containsEpoch(epochs []primitives.Epoch, target primitives.Epoch) bool {
	for _, e := range epochs {
		if e == target {
			return true
		}
	}
	return false
}

// ProposerDutyForSlot exposes the cached schedule to the pubsub validator.
func (c *Chain) ProposerDutyForSlot(slot primitives.Slot) (ProposerDuty, []byte, bool) {
	return c.duties.Lookup(slot)
}

// refreshProposerDuties keeps the current and next epoch cached. Fetching the
// next epoch ahead of time removes the epoch-boundary race where a block
// arrives before its schedule does.
//
// Errors are returned for logging only; a stale cache degrades validation to
// the structural checks rather than stalling relay.
func (c *Chain) refreshProposerDuties(ctx context.Context) error {
	currentEpoch := slots.ToEpoch(slots.CurrentSlot(c.cfg.GenesisConfig.GenesisTime))
	wanted := []primitives.Epoch{currentEpoch, currentEpoch + 1}

	var errs []error
	for _, epoch := range wanted {
		if c.duties.has(epoch) {
			continue
		}

		ed, err := c.fetchEpochDuties(ctx, epoch)
		if err != nil {
			errs = append(errs, fmt.Errorf("epoch %d: %w", epoch, err))
			continue
		}

		c.duties.store(epoch, ed, wanted)
		slog.Debug("cached proposer duties", "epoch", epoch, "slots", len(ed.duties))
	}

	if len(errs) > 0 {
		return fmt.Errorf("refresh proposer duties: %w", errs[0])
	}
	return nil
}

func (c *Chain) fetchEpochDuties(ctx context.Context, epoch primitives.Epoch) (*epochDuties, error) {
	duties, err := c.cfg.clClient.ProposerDuties(ctx, epoch)
	if err != nil {
		return nil, err
	}

	fork, err := params.Fork(epoch)
	if err != nil {
		return nil, fmt.Errorf("fork for epoch: %w", err)
	}

	domain, err := signing.Domain(fork, epoch, params.BeaconConfig().DomainBeaconProposer, c.cfg.GenesisConfig.GenesisValidatorRoot)
	if err != nil {
		return nil, fmt.Errorf("compute beacon proposer domain: %w", err)
	}

	return &epochDuties{duties: duties, domain: domain}, nil
}

// logProposerDutyRefresh runs the refresh and swallows the error after logging,
// so a Prysm outage never takes down the chain loop.
func (c *Chain) logProposerDutyRefresh(ctx context.Context) {
	if err := c.refreshProposerDuties(ctx); err != nil {
		slog.Warn("failed refreshing proposer duties, validation will degrade", tele.LogAttrError(err))
	}
}
