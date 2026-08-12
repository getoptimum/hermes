package host

import (
	"context"
	"log/slog"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel/metric"
)

const (
	// dialBackoffBase is the first penalty applied to a peer that refused us.
	// Deliberately short: a refusal is usually transient, most often the remote
	// being at its inbound peer limit, and banning a good peer for hours over
	// that would cost us more than the wasted dial does.
	dialBackoffBase = time.Minute
	// dialBackoffCacheSize bounds how many peers we remember. The dialer churns
	// through far more than max-peers, so this is sized for the churn, not the
	// connection count.
	dialBackoffCacheSize = 4096
)

// dialBackoffEntry tracks consecutive refusals for one peer.
type dialBackoffEntry struct {
	failures int
	until    time.Time
}

// DialBackoff suppresses redials to peers that recently refused us.
//
// It keys on connection-level refusals, which are observable. It deliberately
// does not try to infer gossip rejection: there is no REJECT on the wire, a peer
// silently debits a local score, and by the time we could infer anything the
// penalty has already been applied. So this reduces wasted dials and peer-pool
// churn; it does not prevent scoring penalties.
type DialBackoff struct {
	mu      sync.Mutex
	entries *lru.Cache[peer.ID, *dialBackoffEntry]
	max     time.Duration
	exempt  func(peer.ID) bool

	meterBlocked metric.Int64Counter
}

func NewDialBackoff(max time.Duration, exempt func(peer.ID) bool, meter metric.Meter) (*DialBackoff, error) {
	entries, err := lru.New[peer.ID, *dialBackoffEntry](dialBackoffCacheSize)
	if err != nil {
		return nil, err
	}

	db := &DialBackoff{
		entries: entries,
		max:     max,
		exempt:  exempt,
	}

	if meter != nil {
		db.meterBlocked, err = meter.Int64Counter(
			"dial_backoff_blocked_total",
			metric.WithDescription("Dials suppressed because the peer recently refused us"),
		)
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

// RecordRefusal notes that a peer refused or dropped us, extending its backoff.
// The growth is exponential so a persistently hostile peer is dropped for long
// enough to matter, while a one-off refusal costs a minute.
func (d *DialBackoff) RecordRefusal(p peer.ID) {
	if d.isExempt(p) {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	entry, ok := d.entries.Get(p)
	if !ok {
		entry = &dialBackoffEntry{}
		d.entries.Add(p, entry)
	}

	entry.failures++
	penalty := dialBackoffBase << min(entry.failures-1, 16)
	if penalty > d.max || penalty <= 0 {
		penalty = d.max
	}
	entry.until = time.Now().Add(penalty)

	slog.Debug("backing off peer after refusal", "peer", p.String(), "failures", entry.failures, "for", penalty)
}

// RecordSuccess clears a peer's history once we hold a usable connection.
func (d *DialBackoff) RecordSuccess(p peer.ID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries.Remove(p)
}

// blocked reports whether p is currently in backoff.
func (d *DialBackoff) blocked(p peer.ID) bool {
	if d.isExempt(p) {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	entry, ok := d.entries.Get(p)
	if !ok {
		return false
	}

	if time.Now().After(entry.until) {
		return false
	}

	return true
}

func (d *DialBackoff) isExempt(p peer.ID) bool {
	return d.exempt != nil && d.exempt(p)
}

func (d *DialBackoff) countBlocked() {
	if d.meterBlocked != nil {
		d.meterBlocked.Add(context.Background(), 1)
	}
}

// InterceptPeerDial implements ConnectionGater.
func (d *DialBackoff) InterceptPeerDial(p peer.ID) bool {
	if d.blocked(p) {
		d.countBlocked()
		return false
	}
	return true
}

// InterceptAddrDial implements ConnectionGater.
func (d *DialBackoff) InterceptAddrDial(p peer.ID, _ ma.Multiaddr) bool {
	return d.InterceptPeerDial(p)
}

// InterceptAccept implements ConnectionGater. Inbound connections are always
// allowed: a peer dialling us is not refusing us.
func (d *DialBackoff) InterceptAccept(network.ConnMultiaddrs) bool { return true }

// InterceptSecured implements ConnectionGater.
func (d *DialBackoff) InterceptSecured(direction network.Direction, p peer.ID, _ network.ConnMultiaddrs) bool {
	if direction == network.DirInbound {
		return true
	}
	return !d.blocked(p)
}

// InterceptUpgraded implements ConnectionGater.
func (d *DialBackoff) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) {
	return true, 0
}
