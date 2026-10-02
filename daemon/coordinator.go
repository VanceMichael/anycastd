package daemon

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/runityru/anycastd/announcer"
)

// serviceHandle is the per-service gate through which the service loop
// reports its advertised state. Multiple services in one generation
// share the same route set, so the coordinator aggregates their state
// instead of letting every service touch the RIB independently: the
// prefixes are announced when at least one service is up and withdrawn
// only when the last up service goes down. This prevents duplicate
// announcements.
type serviceHandle struct {
	up atomic.Bool
}

func newServiceHandle() *serviceHandle {
	return &serviceHandle{}
}

type announceCoordinator struct {
	mu sync.Mutex

	// client resolves route operations to the BGP instance of the
	// currently published generation.
	client announcer.GoBGPServer

	prefixes []string
	nextHop  string

	// up is the set of handles whose services are healthy.
	up map[*serviceHandle]struct{}
}

func newAnnounceCoordinator(client announcer.GoBGPServer) *announceCoordinator {
	return &announceCoordinator{
		client: client,
		up:     map[*serviceHandle]struct{}{},
	}
}

func (c *announceCoordinator) announcerFor(h *serviceHandle) announcer.Announcer {
	return &serviceGate{c: c, h: h}
}

// serviceGate is the announcer given to a service. While a generation
// switch is being staged the gate is inactive: the service may run
// checks and flip its local health state, but none of its transitions
// touch the live RIB. The gate is activated exactly once, inside the
// coordinator's commit critical section, after its handle has been
// adopted. Retired services get their gate deactivated before commit
// so they cannot re-acquire membership after being retired.
type serviceGate struct {
	c    *announceCoordinator
	h    *serviceHandle
	mu   sync.Mutex
	live bool
}

var _ announcer.Announcer = (*serviceGate)(nil)

func (g *serviceGate) activate() {
	g.mu.Lock()
	g.live = true
	g.mu.Unlock()
}

func (g *serviceGate) deactivate() {
	g.mu.Lock()
	g.live = false
	g.mu.Unlock()
}

func (g *serviceGate) isLive() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.live
}

func (g *serviceGate) Announce(ctx context.Context) error {
	if !g.h.up.CompareAndSwap(false, true) {
		return nil
	}
	if g.isLive() {
		return g.c.acquire(ctx, g.h)
	}
	return nil
}

func (g *serviceGate) Denounce(ctx context.Context) error {
	if !g.h.up.CompareAndSwap(true, false) {
		return nil
	}
	if g.isLive() {
		return g.c.release(ctx, g.h)
	}
	return nil
}

// commitGeneration is the single publish point of a generation
// switch. Under one lock it:
//
//  1. retires the handles of removed/replaced old services,
//  2. adopts the health state of staged replacement/new services,
//  3. reconciles the RIB of the target BGP instance to the prefix set
//     implied by the resulting aggregate health,
//  4. swaps the active route client / prefixes / next-hop,
//  5. activates the staged service gates (optionally via the committed
//     callback while still holding the lock).
//
// Nothing is mutated when RIB reconciliation fails. The returned undo
// restores coordinator state and the RIB; callers must deactivate the
// staged gates before invoking it on a failed commit.
func (c *announceCoordinator) commitGeneration(
	ctx context.Context,
	target announcer.GoBGPServer,
	instanceChanged bool,
	newNextHop string,
	newPrefixes []string,
	retire []*serviceHandle,
	adopt []*serviceHandle,
	committed func(),
) (func(context.Context) error, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldClient := c.client
	oldNextHop := c.nextHop
	oldPrefixes := append([]string(nil), c.prefixes...)
	oldUp := make(map[*serviceHandle]struct{}, len(c.up))
	for h := range c.up {
		oldUp[h] = struct{}{}
	}

	newUp := make(map[*serviceHandle]struct{}, len(c.up)+len(adopt))
	for h := range c.up {
		newUp[h] = struct{}{}
	}
	for _, h := range retire {
		delete(newUp, h)
	}
	for _, h := range adopt {
		if h.up.Load() {
			newUp[h] = struct{}{}
		}
	}

	beforeAdvertised := len(oldUp) > 0
	afterAdvertised := len(newUp) > 0

	oldRIB := []string{}
	if beforeAdvertised {
		oldRIB = append(oldRIB, oldPrefixes...)
	}
	desiredRIB := []string{}
	if afterAdvertised {
		desiredRIB = append(desiredRIB, newPrefixes...)
	}

	routeUndo, err := reconcileRoutes(ctx, target, oldNextHop, oldRIB, newNextHop, desiredRIB)
	if err != nil {
		return nil, err
	}

	c.client = target
	c.prefixes = append([]string(nil), newPrefixes...)
	c.nextHop = newNextHop
	c.up = newUp

	if committed != nil {
		committed()
	}

	return func(undoCtx context.Context) error {
		c.mu.Lock()
		defer c.mu.Unlock()

		if instanceChanged {
			// The old instance kept its RIB; only drop what was staged
			// on the new instance.
			if err := routeUndo(undoCtx); err != nil {
				return err
			}
		} else {
			// Membership has been restored to oldUp; honor the aggregate
			// health observed at undo time rather than the commit instant.
			restoreRIB := []string{}
			if beforeAdvertised && len(c.up) > 0 {
				restoreRIB = append(restoreRIB, oldPrefixes...)
			}
			if _, err := reconcileRoutes(undoCtx, target, newNextHop, desiredRIB, oldNextHop, restoreRIB); err != nil {
				return err
			}
		}

		c.client = oldClient
		c.prefixes = oldPrefixes
		c.nextHop = oldNextHop
		c.up = oldUp
		return nil
	}, nil
}

// acquire marks a handle as up. The first up service triggers a single
// announcement of the whole generation prefix set.
func (c *announceCoordinator) acquire(ctx context.Context, h *serviceHandle) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.up[h]; ok {
		return nil
	}

	first := len(c.up) == 0
	c.up[h] = struct{}{}
	if !first || c.client == nil || len(c.prefixes) == 0 {
		return nil
	}

	if err = announceAll(ctx, c.client, c.prefixes, c.nextHop); err != nil {
		delete(c.up, h)
	}
	return err
}

// release marks a handle as down. Only the departure of the last up
// service withdraws the prefix set.
func (c *announceCoordinator) release(ctx context.Context, h *serviceHandle) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.up[h]; !ok {
		return nil
	}

	delete(c.up, h)
	if len(c.up) > 0 || c.client == nil || len(c.prefixes) == 0 {
		return nil
	}

	if err = withdrawAll(ctx, c.client, c.prefixes); err != nil {
		c.up[h] = struct{}{}
	}
	return err
}

// adopt transfers a handle into the up set without any RIB operation.
// It is used when a healthy service is replaced during a generation
// switch: the replacement inherits the advertised state, so there is
// neither a withdrawal nor a second announcement.
func (c *announceCoordinator) adopt(h *serviceHandle, isUp bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if isUp {
		c.up[h] = struct{}{}
	}
}

// evict removes a handle from the up set without any RIB operation. It
// is used for replaced services whose advertisement has already been
// inherited by their replacement.
func (c *announceCoordinator) evict(h *serviceHandle) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.up, h)
}

func (c *announceCoordinator) snapshot() (client announcer.GoBGPServer, prefixes []string, nextHop string, advertised bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client, append([]string(nil), c.prefixes...), c.nextHop, len(c.up) > 0
}

func announceAll(ctx context.Context, client announcer.GoBGPServer, prefixes []string, nextHop string) error {
	for _, p := range prefixes {
		if err := client.AddPath(ctx, p, nextHop); err != nil {
			return errors.Wrapf(err, "error announcing %s", p)
		}
	}
	return nil
}

func withdrawAll(ctx context.Context, client announcer.GoBGPServer, prefixes []string) error {
	for _, p := range prefixes {
		if err := client.DeletePath(ctx, p); err != nil {
			return errors.Wrapf(err, "error withdrawing %s", p)
		}
	}
	return nil
}

// primitive is one irreversible-ish RIB call paired with its inverse.
type routePrimitive struct {
	apply  func(context.Context) error
	revert func(context.Context) error
}

// reconcileRoutes transforms the prefix set on target from old to new,
// honoring a next-hop change for retained prefixes. Every executed
// primitive is recorded; on failure the recorded inverses run in
// reverse order, so a partially applied set never survives an error.
func reconcileRoutes(
	ctx context.Context,
	target announcer.GoBGPServer,
	oldNextHop string,
	oldPrefixes []string,
	newNextHop string,
	newPrefixes []string,
) (func(context.Context) error, error) {
	oldSet := map[string]struct{}{}
	for _, p := range oldPrefixes {
		oldSet[p] = struct{}{}
	}
	newSet := map[string]struct{}{}
	for _, p := range newPrefixes {
		newSet[p] = struct{}{}
	}

	var ops []routePrimitive

	add := func(prefix, nextHop string) {
		ops = append(ops, routePrimitive{
			apply:  func(ctx context.Context) error { return target.AddPath(ctx, prefix, nextHop) },
			revert: func(ctx context.Context) error { return target.DeletePath(ctx, prefix) },
		})
	}
	del := func(prefix, prevNextHop string) {
		ops = append(ops, routePrimitive{
			apply:  func(ctx context.Context) error { return target.DeletePath(ctx, prefix) },
			revert: func(ctx context.Context) error { return target.AddPath(ctx, prefix, prevNextHop) },
		})
	}

	// Deterministic order: adds first (reachability gained before the
	// old copy is removed), then retained-prefix replacements, finally
	// removals.
	for _, p := range newPrefixes {
		if _, ok := oldSet[p]; !ok {
			add(p, newNextHop)
		}
	}
	for _, p := range newPrefixes {
		if _, ok := oldSet[p]; ok && oldNextHop != newNextHop {
			del(p, oldNextHop)
			add(p, newNextHop)
		}
	}
	for _, p := range oldPrefixes {
		if _, ok := newSet[p]; !ok {
			del(p, oldNextHop)
		}
	}

	applied := make([]routePrimitive, 0, len(ops))
	for _, op := range ops {
		if err := op.apply(ctx); err != nil {
			for i := len(applied) - 1; i >= 0; i-- {
				if rerr := applied[i].revert(ctx); rerr != nil {
					return nil, errors.Wrapf(err, "error reconciling routes (rollback also failed: %s)", rerr)
				}
			}
			return nil, errors.Wrap(err, "error reconciling routes")
		}
		applied = append(applied, op)
	}

	return func(undoCtx context.Context) error {
		for i := len(applied) - 1; i >= 0; i-- {
			if err := applied[i].revert(undoCtx); err != nil {
				return errors.Wrap(err, "error reverting route reconciliation")
			}
		}
		return nil
	}, nil
}
