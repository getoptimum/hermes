package host

import (
	"sync"

	connmgr "github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// deferredGater is a ConnectionGater that delegates all calls to an actual gater
// once it's set. This allows us to create the gater before we have the host.
type deferredGater struct {
	actual connmgr.ConnectionGater
	mu     sync.RWMutex
}

// newDeferredGater creates a new deferred connection gater
func newDeferredGater() *deferredGater {
	return &deferredGater{}
}

// compositeGater requires every delegate to allow a connection. Any one of them
// vetoing is enough to block, so gaters can be composed without knowing about
// each other.
type compositeGater struct {
	gaters []connmgr.ConnectionGater
}

func newCompositeGater(gaters ...connmgr.ConnectionGater) connmgr.ConnectionGater {
	present := make([]connmgr.ConnectionGater, 0, len(gaters))
	for _, g := range gaters {
		if g != nil {
			present = append(present, g)
		}
	}

	if len(present) == 1 {
		return present[0]
	}
	return &compositeGater{gaters: present}
}

func (cg *compositeGater) InterceptPeerDial(p peer.ID) bool {
	for _, g := range cg.gaters {
		if !g.InterceptPeerDial(p) {
			return false
		}
	}
	return true
}

func (cg *compositeGater) InterceptAddrDial(p peer.ID, addr ma.Multiaddr) bool {
	for _, g := range cg.gaters {
		if !g.InterceptAddrDial(p, addr) {
			return false
		}
	}
	return true
}

func (cg *compositeGater) InterceptAccept(conn network.ConnMultiaddrs) bool {
	for _, g := range cg.gaters {
		if !g.InterceptAccept(conn) {
			return false
		}
	}
	return true
}

func (cg *compositeGater) InterceptSecured(direction network.Direction, p peer.ID, conn network.ConnMultiaddrs) bool {
	for _, g := range cg.gaters {
		if !g.InterceptSecured(direction, p, conn) {
			return false
		}
	}
	return true
}

func (cg *compositeGater) InterceptUpgraded(conn network.Conn) (bool, control.DisconnectReason) {
	for _, g := range cg.gaters {
		if allow, reason := g.InterceptUpgraded(conn); !allow {
			return false, reason
		}
	}
	return true, 0
}

// SetActual sets the actual connection gater to delegate to
func (dg *deferredGater) SetActual(gater connmgr.ConnectionGater) {
	dg.mu.Lock()
	defer dg.mu.Unlock()
	dg.actual = gater
}

// InterceptPeerDial implements ConnectionGater
func (dg *deferredGater) InterceptPeerDial(p peer.ID) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual != nil {
		return dg.actual.InterceptPeerDial(p)
	}
	return true // Allow by default if no actual gater is set
}

// InterceptAddrDial implements ConnectionGater
func (dg *deferredGater) InterceptAddrDial(p peer.ID, addr ma.Multiaddr) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual != nil {
		return dg.actual.InterceptAddrDial(p, addr)
	}
	return true
}

// InterceptAccept implements ConnectionGater
func (dg *deferredGater) InterceptAccept(conn network.ConnMultiaddrs) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual != nil {
		return dg.actual.InterceptAccept(conn)
	}
	return true
}

// InterceptSecured implements ConnectionGater
func (dg *deferredGater) InterceptSecured(direction network.Direction, p peer.ID, conn network.ConnMultiaddrs) bool {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual != nil {
		return dg.actual.InterceptSecured(direction, p, conn)
	}
	return true
}

// InterceptUpgraded implements ConnectionGater
func (dg *deferredGater) InterceptUpgraded(conn network.Conn) (bool, control.DisconnectReason) {
	dg.mu.RLock()
	defer dg.mu.RUnlock()
	if dg.actual != nil {
		return dg.actual.InterceptUpgraded(conn)
	}
	return true, 0
}
