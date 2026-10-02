// Package daemon implements the anycastd runtime: one long-lived
// process that owns a GoBGP instance, a set of service check loops and
// the metrics endpoint, and that switches between configuration
// generations atomically in response to SIGHUP.
//
// A reload is a three-stage transaction:
//
//	load -> normalize/validate/build (failure isolated, old generation
//	kept running) -> stage additive runtime changes with inverses ->
//	commit the RIB/peer/metrics publication under the coordinator lock
//	-> retire old resources. Any staging or commit failure triggers
//	compensation of every executed change, so the previous generation
//	remains externally consistent.
package daemon

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	apipb "github.com/osrg/gobgp/v3/api"
	log "github.com/sirupsen/logrus"

	"github.com/runityru/anycastd/config"
	"github.com/runityru/anycastd/service"
)

// Options configures a Daemon.
type Options struct {
	// ConfigPath is the configuration file re-read on every reload.
	ConfigPath string
	// LogLevel is propagated to BGP instances created on global changes.
	LogLevel log.Level
	// AppVersion labels the process liveness metric.
	AppVersion string
	// NewBGP creates fresh BGP server instances.
	NewBGP BGPFactory
	// NewRouteClient creates the dynamic route client through which
	// service-driven announcements reach the active BGP instance.
	NewRouteClient RouteClientFactory
}

// Daemon owns the process-wide runtime and serializes configuration
// generation switches.
type Daemon struct {
	opts Options

	rootCtx    context.Context
	lifeCtx    context.Context
	lifeCancel context.CancelFunc

	// reloadMu serializes reloads. Shutdown acquires it to wait for an
	// in-flight reload (including its compensation) to finish.
	reloadMu sync.Mutex

	closeOnce sync.Once

	stateMu sync.RWMutex
	active  *generation
	genSeq  uint64
	attempt uint64

	coord   *announceCoordinator
	proxy   *metricsProxy
	metrics *metricsSupervisor

	svcWg sync.WaitGroup

	instancesMu sync.Mutex
	instances   map[*bgpRuntime]struct{}

	statusMu sync.Mutex
	status   reloadStatus
}

// generation is one fully published configuration.
type generation struct {
	id       uint64
	cfg      *config.Config
	fp       string
	bgp      *bgpRuntime
	globalFP string

	peers    map[string]config.Peer
	prefixes []string

	// prepared is the validated construction result keyed by service
	// name; services holds the live goroutines/gates.
	prepared map[string]preparedService
	services map[string]*managedService
}

// managedService is one running check loop.
type managedService struct {
	name   string
	gate   *serviceGate
	handle *serviceHandle
	cancel context.CancelFunc
	done   chan struct{}
}

// bgpRuntime is a BGP instance together with its event watch.
type bgpRuntime struct {
	inst        BGPInstance
	watchCancel context.CancelFunc
	watchDone   chan struct{}
	closed      atomic.Bool
}

// New constructs a Daemon.
func New(opts Options) *Daemon {
	return &Daemon{
		opts:      opts,
		instances: map[*bgpRuntime]struct{}{},
	}
}

// ActiveGeneration returns the sequence number of the generation
// currently serving traffic.
func (d *Daemon) ActiveGeneration() uint64 {
	d.stateMu.RLock()
	defer d.stateMu.RUnlock()
	if d.active == nil {
		return 0
	}
	return d.active.id
}

// LastStatus returns a copy of the most recent reload status.
func (d *Daemon) LastStatus() reloadStatus {
	d.statusMu.Lock()
	defer d.statusMu.Unlock()
	return d.status
}

// Run starts the daemon with the initial configuration and blocks
// until ctx is canceled (SIGINT/SIGTERM). SIGHUP notifications are
// delivered through reloadCh: the channel may coalesce multiple
// signals, reloads never run concurrently.
func (d *Daemon) Run(ctx context.Context, reloadCh <-chan struct{}) error {
	d.rootCtx = ctx
	d.lifeCtx, d.lifeCancel = context.WithCancel(context.Background())

	d.proxy = newMetricsProxy()
	d.metrics = newMetricsSupervisor(d.opts.AppVersion, d.lifeCtx, d.proxy)

	initialCfg, err := config.NewFromFile(d.opts.ConfigPath)
	if err != nil {
		return errors.Wrap(err, "error loading initial configuration")
	}

	initial, err := prepare(initialCfg)
	if err != nil {
		return errors.Wrap(err, "error preparing initial configuration")
	}

	// Dynamic route client resolving to whatever instance is active.
	routeClient := d.opts.NewRouteClient(func() BGPClient {
		d.stateMu.RLock()
		defer d.stateMu.RUnlock()
		if d.active == nil || d.active.bgp == nil {
			return nil
		}
		return d.active.bgp.inst.Client
	})
	d.coord = newAnnounceCoordinator(routeClient)

	// Bootstrap is the same transaction applied to an empty
	// generation; a failure prevents the process from serving.
	empty := &generation{
		prepared: map[string]preparedService{},
		services: map[string]*managedService{},
		peers:    map[string]config.Peer{},
	}
	d.stateMu.Lock()
	d.active = empty
	d.stateMu.Unlock()

	bootstrapCtx, bootstrapCancel := context.WithCancel(ctx)
	defer bootstrapCancel()

	if err := d.switchGeneration(bootstrapCtx, initial, true); err != nil {
		d.shutdownResources(context.Background())
		return errors.Wrap(err, "error applying initial configuration")
	}
	d.recordSuccess(0, reloadApplied)

	log.Infof("initialization completed, running generation %d", d.ActiveGeneration())

	for {
		select {
		case <-ctx.Done():
			d.shutdown()
			return nil
		case <-reloadCh:
			d.consumeReload(ctx, reloadCh)
		}
	}
}

// Reload triggers one generation switch programmatically. It is safe
// for concurrent use; calls are serialized.
func (d *Daemon) Reload(ctx context.Context) error {
	d.reloadMu.Lock()
	defer d.reloadMu.Unlock()
	return d.doReload(ctx)
}

// consumeReload processes one coalesced SIGHUP batch: signals that
// arrive while a reload is in progress (or between iterations) cause
// exactly one additional reload, never concurrent work.
func (d *Daemon) consumeReload(ctx context.Context, reloadCh <-chan struct{}) {
	d.reloadMu.Lock()
	defer d.reloadMu.Unlock()

	for {
		_ = d.doReload(ctx)
		select {
		case <-ctx.Done():
			return
		case <-reloadCh:
			// A signal arrived while we were reloading: re-read the file
			// once more, then settle.
			continue
		default:
			return
		}
	}
}

func (d *Daemon) nextAttempt() uint64 {
	d.attempt++
	return d.attempt
}

func (d *Daemon) recordSuccess(attempt uint64, result string) {
	d.stateMu.RLock()
	gen := d.genSeq
	d.stateMu.RUnlock()

	st := reloadStatus{
		generation: gen,
		successful: true,
		timestamp:  time.Now(),
		result:     result,
	}
	d.statusMu.Lock()
	d.status = st
	d.statusMu.Unlock()
	d.metrics.publishStatus(st)

	log.WithFields(log.Fields{
		"generation": gen,
		"attempt":    attempt,
		"result":     result,
	}).Info("configuration reload completed")
}

func (d *Daemon) recordFailure(attempt uint64, stage string, cause error) {
	d.stateMu.RLock()
	gen := d.genSeq
	d.stateMu.RUnlock()

	reason := stage
	if cause != nil {
		reason = stage + ": " + cause.Error()
	}
	st := reloadStatus{
		generation: gen,
		successful: false,
		timestamp:  time.Now(),
		result:     reloadFailed,
		reason:     reason,
	}
	d.statusMu.Lock()
	d.status = st
	d.statusMu.Unlock()
	d.metrics.publishStatus(st)

	log.WithFields(log.Fields{
		"generation": gen,
		"attempt":    attempt,
		"stage":      stage,
	}).WithError(cause).Error("configuration reload failed; continuing with the previous generation")
}

func (d *Daemon) doReload(ctx context.Context) error {
	attempt := d.nextAttempt()

	log.WithField("attempt", attempt).Info("configuration reload initiated")

	cfg, err := config.NewFromFile(d.opts.ConfigPath)
	if err != nil {
		d.recordFailure(attempt, "load", err)
		return err
	}

	cand, err := prepare(cfg)
	if err != nil {
		d.recordFailure(attempt, "validate", err)
		return err
	}

	d.stateMu.RLock()
	activeFP := ""
	if d.active != nil {
		activeFP = d.active.fp
	}
	d.stateMu.RUnlock()

	if cand.fingerprint == activeFP {
		log.WithField("attempt", attempt).Info("configuration unchanged; reload is a no-op")
		d.recordSuccess(attempt, reloadUnchanged)
		return nil
	}

	if err := d.switchGeneration(ctx, cand, false); err != nil {
		d.recordFailure(attempt, "apply", err)
		return err
	}

	d.recordSuccess(attempt, reloadApplied)
	return nil
}

// switchGeneration runs one staged transaction against the active
// generation. On failure every staged change is compensated and the
// active generation is left untouched.
func (d *Daemon) switchGeneration(ctx context.Context, cand *candidate, bootstrap bool) (err error) {
	actx, cancel := context.WithCancel(ctx)
	defer cancel()

	d.stateMu.RLock()
	active := d.active
	d.stateMu.RUnlock()

	t := &txn{
		d:         d,
		cand:      cand,
		bootstrap: bootstrap,
		active:    active,
	}

	committed := false
	defer func() {
		if !committed {
			// Compensation uses a detached context: when the abort was
			// caused by shutdown the parent context is already canceled,
			// but restoring the old generation must still finish.
			rbCtx, rbCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer rbCancel()
			t.rollback(rbCtx)
		}
	}()

	// --- Stage 1: BGP instance when global config changed -------------
	target := active.bgp
	instanceChanged := false
	if active.bgp == nil || active.globalFP != cand.globalFP {
		rt, err := d.startBGP(actx, cand)
		if err != nil {
			return errors.Wrap(err, "error starting BGP instance")
		}
		target = rt
		instanceChanged = true
		t.newBGP = rt
		t.appendStageUndo(func(undoCtx context.Context) error {
			d.closeRuntime(undoCtx, rt)
			return nil
		})
	}
	t.target = target

	// --- Stage 2: peers (add / replace / delete by stable name) -------
	// On a BGP instance replacement the old sessions disappear together
	// with the closed instance (StopBgp tears down every neighbor), so the
	// new instance starts from an empty peer set.
	oldPeers := active.peers
	if instanceChanged {
		oldPeers = map[string]config.Peer{}
	}
	oldLocalAddress := ""
	if active.cfg != nil {
		oldLocalAddress = active.cfg.Announcer.LocalAddress
	}
	if err := d.applyPeerOps(actx, t, target, oldPeers, cand.peers, oldLocalAddress, cand.cfg.Announcer.LocalAddress); err != nil {
		return errors.Wrap(err, "error reconciling peers")
	}

	// --- Stage 3: stage new and replaced services ----------------------
	if err := d.stageServices(actx, t, cand); err != nil {
		return errors.Wrap(err, "error staging services")
	}

	// --- Stage 4: metrics (additive part only) -------------------------
	newPeer := func() (PeerMetrics, error) {
		return target.inst.NewPeerMetrics(cand.cfg.Announcer.RouterID, cand.cfg.Announcer.LocalASN)
	}
	d.statusMu.Lock()
	initialStatus := d.status
	d.statusMu.Unlock()
	initialStatus.successful = true
	mtxn := d.metrics.plan(metricsView{
		Enabled:       cand.cfg.Metrics.Enabled,
		Address:       cand.cfg.Metrics.Address,
		InitialStatus: initialStatus,
	}, cand.cfg.Announcer.RouterID, cand.cfg.Announcer.LocalASN, newPeer)
	t.metricsTxn = &mtxn
	if err := mtxn.stage(actx); err != nil {
		return errors.Wrap(err, "error staging metrics")
	}
	t.appendStageUndo(func(undoCtx context.Context) error {
		mtxn.rollback(undoCtx)
		return nil
	})

	// ===================== COMMIT =====================

	// Retire old services' gates first: they keep running (and can be
	// fully restored on rollback), but they can no longer alter RIB
	// membership during the commit.
	var retired []*managedService
	retireHandles := []*serviceHandle{}
	{
		removed, replaced := serviceDiffs(active, cand)
		for _, name := range append(append([]string{}, removed...), replaced...) {
			if ms := active.services[name]; ms != nil {
				ms.gate.deactivate()
				retired = append(retired, ms)
				retireHandles = append(retireHandles, ms.handle)
			}
		}
		t.retired = retired
	}
	for _, ms := range retired {
		deferGate := ms.gate
		t.appendCommitUndo(func(context.Context) error {
			deferGate.activate()
			return nil
		})
	}

	// Commit routes and service membership atomically; staged gates
	// become live inside the same critical section.
	routeUndo, err := d.coord.commitGeneration(
		actx,
		target.inst.Routes,
		instanceChanged,
		cand.cfg.Announcer.LocalAddress,
		cand.prefixes,
		retireHandles,
		t.stagedHandles(),
		func() {
			for _, g := range t.stagedGates() {
				g.activate()
			}
		},
	)
	if err != nil {
		return errors.Wrap(err, "error committing routes")
	}
	t.appendCommitUndo(func(undoCtx context.Context) error {
		for _, g := range t.stagedGates() {
			g.deactivate()
		}
		return routeUndo(undoCtx)
	})

	// Metrics visibility swap.
	if err := mtxn.commit(actx); err != nil {
		return errors.Wrap(err, "error committing metrics")
	}
	t.appendCommitUndo(func(undoCtx context.Context) error {
		// Best-effort reverse of committed metrics transitions: proxy
		// unbind and disable, or restore the old HTTP endpoint.
		d.metrics.mu.Lock()
		defer d.metrics.mu.Unlock()
		if cand.cfg.Metrics.Enabled && !activeHasMetrics(active) {
			d.proxy.set(nil)
			d.metrics.teardownLocked(undoCtx)
			d.metrics.enabled = false
			d.metrics.address = ""
			d.metrics.routerID = ""
			d.metrics.asn = 0
		}
		return nil
	})

	// Old services stop only after every failure-prone step succeeded.
	for _, ms := range retired {
		ms.cancel()
		<-ms.done
	}

	// Publish the new generation. Unchanged services keep both their
	// running goroutine and their already-constructed checkers; only
	// added/replaced services carry the freshly prepared definitions.
	newGen := &generation{
		id:        d.genSeq + 1,
		cfg:       cand.cfg,
		fp:        cand.fingerprint,
		bgp:       target,
		globalFP:  cand.globalFP,
		peers:     cand.peers,
		prefixes:  append([]string(nil), cand.prefixes...),
		prepared:  t.combinedPrepared(active),
		services:  t.combinedServices(active),
	}

	d.stateMu.Lock()
	d.active = newGen
	d.genSeq = newGen.id
	d.stateMu.Unlock()
	committed = true

	// Post-commit cleanup that cannot invalidate the new generation.
	removedNames, _ := serviceDiffs(active, cand)
	for _, name := range removedNames {
		d.metrics.deleteServiceLabels(name)
	}

	if instanceChanged && active.bgp != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		d.closeRuntime(closeCtx, active.bgp)
		closeCancel()
	}

	log.WithFields(log.Fields{
		"generation": newGen.id,
		"services":   len(newGen.services),
		"peers":      len(newGen.peers),
		"prefixes":   len(newGen.prefixes),
	}).Info("configuration generation published")

	return nil
}

func activeHasMetrics(g *generation) bool {
	if g == nil || g.cfg == nil {
		return false
	}
	return g.cfg.Metrics.Enabled
}

// txn carries the staged state and compensation stacks of one
// generation switch.
type txn struct {
	d         *Daemon
	cand      *candidate
	active    *generation
	bootstrap bool

	target      *bgpRuntime
	newBGP      *bgpRuntime
	stageUndos  []func(context.Context) error
	commitUndos []func(context.Context) error

	staged []*managedService

	// retired old services, kept for the commit phase.
	retired []*managedService

	metricsTxn *metricsTxn
}

func (t *txn) appendStageUndo(fn func(context.Context) error) {
	t.stageUndos = append(t.stageUndos, fn)
}

func (t *txn) appendCommitUndo(fn func(context.Context) error) {
	t.commitUndos = append(t.commitUndos, fn)
}

func (t *txn) stagedGates() []*serviceGate {
	out := make([]*serviceGate, 0, len(t.staged))
	for _, ms := range t.staged {
		out = append(out, ms.gate)
	}
	return out
}

func (t *txn) stagedHandles() []*serviceHandle {
	out := make([]*serviceHandle, 0, len(t.staged))
	for _, ms := range t.staged {
		out = append(out, ms.handle)
	}
	return out
}

func (t *txn) combinedServices(active *generation) map[string]*managedService {
	out := make(map[string]*managedService, len(t.cand.services))
	for name := range t.cand.services {
		if ms := active.services[name]; ms != nil {
			if oldPrep, ok := active.prepared[name]; ok {
				if newPrep, ok := t.cand.services[name]; ok && oldPrep.fp == newPrep.fp {
					out[name] = ms
					continue
				}
			}
		}
	}
	for _, ms := range t.staged {
		out[ms.name] = ms
	}
	return out
}

// combinedPrepared returns the checker/strategy set that survives the
// switch: the previously constructed objects for unchanged services
// (so stable identities are preserved), the candidate objects for added
// and replaced ones.
func (t *txn) combinedPrepared(active *generation) map[string]preparedService {
	out := make(map[string]preparedService, len(t.cand.services))
	for name, newPrep := range t.cand.services {
		if oldPrep, ok := active.prepared[name]; ok && oldPrep.fp == newPrep.fp {
			out[name] = oldPrep
			continue
		}
		out[name] = newPrep
	}
	return out
}

// rollback reverses committed changes first (in reverse order), then
// staged changes. It is idempotent across compensation kinds.
func (t *txn) rollback(ctx context.Context) {
	for i := len(t.commitUndos) - 1; i >= 0; i-- {
		if err := t.commitUndos[i](ctx); err != nil {
			log.WithError(err).Warn("error while compensating committed generation change")
		}
	}

	// Staged services never became part of the active generation; their
	// gates were inactive so the coordinator owns none of their state.
	for _, ms := range t.staged {
		ms.cancel()
		<-ms.done
	}

	for i := len(t.stageUndos) - 1; i >= 0; i-- {
		if err := t.stageUndos[i](ctx); err != nil {
			log.WithError(err).Warn("error while compensating staged generation change")
		}
	}
}

// serviceDiffs returns removed and replaced service names.
func serviceDiffs(active *generation, cand *candidate) (removed, replaced []string) {
	for name := range active.prepared {
		newPrep, ok := cand.services[name]
		if !ok {
			removed = append(removed, name)
			continue
		}
		if active.prepared[name].fp != newPrep.fp {
			replaced = append(replaced, name)
		}
	}
	sort.Strings(removed)
	sort.Strings(replaced)
	return removed, replaced
}

func addedOrReplacedServices(active *generation, cand *candidate) []string {
	var names []string
	for name, newPrep := range cand.services {
		oldPrep, ok := active.prepared[name]
		if !ok || oldPrep.fp != newPrep.fp {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// stageServices starts the goroutines for added and replaced services.
// Gates are inactive until commit, so no check result can touch the
// live RIB during staging.
func (d *Daemon) stageServices(ctx context.Context, t *txn, cand *candidate) error {
	for _, name := range addedOrReplacedServices(t.active, cand) {
		prep := cand.services[name]

		h := newServiceHandle()
		gate := &serviceGate{c: d.coord, h: h}

		sctx, scancel := context.WithCancel(d.lifeCtx)
		ms := &managedService{
			name:   name,
			gate:   gate,
			handle: h,
			cancel: scancel,
			done:   make(chan struct{}),
		}

		svc := service.New(
			name,
			gate,
			prep.checks,
			prep.cfg.CheckInterval.TimeDuration(),
			d.proxy,
			prep.strategy,
		)

		d.svcWg.Add(1)
		go func() {
			defer d.svcWg.Done()
			_ = svc.Run(sctx)
			close(ms.done)
		}()

		t.staged = append(t.staged, ms)
	}
	return nil
}

// peerPrimitive is one peer mutation with its inverse.
type peerPrimitive struct {
	desc string
	apply func(context.Context) error
	undo  func(context.Context) error
}

func (d *Daemon) applyPeerOps(
	ctx context.Context,
	t *txn,
	rt *bgpRuntime,
	oldPeers, newPeers map[string]config.Peer,
	oldLocalAddress, newLocalAddress string,
) error {
	ops := planPeerOps(rt, oldPeers, newPeers, oldLocalAddress, newLocalAddress)

	for _, op := range ops {
		if err := op.apply(ctx); err != nil {
			return errors.Wrapf(err, "error executing peer operation `%s`", op.desc)
		}
		undo := op.undo
		t.appendStageUndo(func(undoCtx context.Context) error {
			if err := undo(undoCtx); err != nil {
				return errors.Wrapf(err, "error reverting peer operation `%s`", op.desc)
			}
			return nil
		})
	}
	return nil
}

// planPeerOps builds a deterministic, compensatable peer mutation
// plan. Peers are correlated by their stable Name; a neighbor-address
// change (or any parameter change) is implemented as delete+add, with
// conflicting deletes scheduled before the conflicting adds.
func planPeerOps(rt *bgpRuntime, oldPeers, newPeers map[string]config.Peer, oldLocalAddress, newLocalAddress string) []peerPrimitive {
	newAddrs := map[string]struct{}{}
	for _, p := range newPeers {
		newAddrs[p.RemoteAddress] = struct{}{}
	}

	oldNames := sortedPeerNames(oldPeers)
	newNames := sortedPeerNames(newPeers)

	var ops []peerPrimitive
	earlyDeleted := map[string]bool{}

	add := func(p config.Peer) peerPrimitive {
		return peerPrimitive{
			desc: "add:" + p.Name,
			apply: func(ctx context.Context) error {
				return rt.inst.Client.AddPeer(ctx, &apipb.AddPeerRequest{
					Peer: BuildPeer(p, newLocalAddress),
				})
			},
			undo: func(ctx context.Context) error {
				return rt.inst.Client.DeletePeer(ctx, &apipb.DeletePeerRequest{
					Address: p.RemoteAddress,
				})
			},
		}
	}
	del := func(p config.Peer) peerPrimitive {
		return peerPrimitive{
			desc: "delete:" + p.Name,
			apply: func(ctx context.Context) error {
				return rt.inst.Client.DeletePeer(ctx, &apipb.DeletePeerRequest{
					Address: p.RemoteAddress,
				})
			},
			undo: func(ctx context.Context) error {
				old := p
				if orig, ok := oldPeers[p.Name]; ok {
					old = orig
				}
				return rt.inst.Client.AddPeer(ctx, &apipb.AddPeerRequest{
					Peer: BuildPeer(old, oldLocalAddress),
				})
			},
		}
	}

	// Phase 0: free addresses that are going to be reused by desired
	// peers.
	for _, name := range oldNames {
		oldP := oldPeers[name]
		newP, exists := newPeers[name]
		leaving := !exists || newP.RemoteAddress != oldP.RemoteAddress
		if !leaving {
			continue
		}
		if _, conflict := newAddrs[oldP.RemoteAddress]; conflict {
			ops = append(ops, del(oldP))
			earlyDeleted[name] = true
		}
	}

	// Phase 1: additions and same-address replacements.
	for _, name := range newNames {
		newP := newPeers[name]
		oldP, exists := oldPeers[name]
		if !exists {
			ops = append(ops, add(newP))
			continue
		}
		if oldP == newP {
			continue
		}
		if oldP.RemoteAddress == newP.RemoteAddress && !earlyDeleted[name] {
			ops = append(ops, del(oldP))
			ops = append(ops, add(newP))
		} else {
			// Different address: old side is deleted in phase 2 (or was
			// deleted in phase 0 because the address is reused).
			ops = append(ops, add(newP))
		}
	}

	// Phase 2: remaining removals.
	for _, name := range oldNames {
		oldP := oldPeers[name]
		if earlyDeleted[name] {
			continue
		}
		newP, keep := newPeers[name]
		if !keep || newP.RemoteAddress != oldP.RemoteAddress {
			ops = append(ops, del(oldP))
		}
	}

	return ops
}

func sortedPeerNames(peers map[string]config.Peer) []string {
	names := make([]string, 0, len(peers))
	for name := range peers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// startBGP creates, serves and starts a BGP instance and subscribes it
// to peer state events.
func (d *Daemon) startBGP(ctx context.Context, cand *candidate) (*bgpRuntime, error) {
	inst, err := d.opts.NewBGP(d.opts.LogLevel)
	if err != nil {
		return nil, err
	}

	// Serve never returns after StopBgp in gobgp v3; run it untracked so
	// shutdown does not wait for a parked goroutine.
	go inst.Serve()

	if err := inst.Client.StartBgp(ctx, &apipb.StartBgpRequest{
		Global: &apipb.Global{
			RouterId:   cand.cfg.Announcer.RouterID,
			Asn:        cand.cfg.Announcer.LocalASN,
			ListenPort: -1,
		},
	}); err != nil {
		_ = inst.Close(context.Background())
		return nil, err
	}

	wctx, watchCancel := context.WithCancel(context.Background())
	watchDone := make(chan struct{})
	rt := &bgpRuntime{inst: inst, watchCancel: watchCancel, watchDone: watchDone}

	d.instancesMu.Lock()
	d.instances[rt] = struct{}{}
	d.instancesMu.Unlock()

	go func() {
		defer close(watchDone)
		err := inst.Client.WatchEvent(wctx, &apipb.WatchEventRequest{
			Peer: &apipb.WatchEventRequest_Peer{},
		}, func(r *apipb.WatchEventResponse) {
			if p := r.GetPeer(); p != nil && p.Type == apipb.WatchEventResponse_PeerEvent_STATE {
				log.Info(p)
			}
		})
		if err != nil && wctx.Err() == nil {
			log.WithError(err).Debug("BGP event watch terminated")
		}
	}()

	return rt, nil
}

func (d *Daemon) closeRuntime(ctx context.Context, rt *bgpRuntime) {
	if rt == nil {
		return
	}
	if !rt.closed.CompareAndSwap(false, true) {
		return
	}
	rt.watchCancel()
	<-rt.watchDone
	if err := rt.inst.Close(ctx); err != nil {
		log.WithError(err).Warn("error stopping BGP instance")
	}

	d.instancesMu.Lock()
	delete(d.instances, rt)
	d.instancesMu.Unlock()
}

// shutdown performs process termination exactly once: cancel service
// loops, wait for any in-flight reload (and its compensation) to
// finish, then stop the metrics endpoint and every BGP instance.
func (d *Daemon) shutdown() {
	d.closeOnce.Do(func() {
		log.Info("shutting down ...")

		// Stop all service loops first.
		d.lifeCancel()

		// Wait for an in-flight reload to finish, including compensation
		// triggered by the cancellation.
		d.reloadMu.Lock()
		defer d.reloadMu.Unlock()

		d.svcWg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		d.metrics.shutdown(ctx)

		d.instancesMu.Lock()
		runtimes := make([]*bgpRuntime, 0, len(d.instances))
		for rt := range d.instances {
			runtimes = append(runtimes, rt)
		}
		d.instancesMu.Unlock()

		for _, rt := range runtimes {
			d.closeRuntime(ctx, rt)
		}

		log.Info("shutdown completed")
	})
}

// shutdownResources is used when bootstrapping fails before the signal
// loop exists.
func (d *Daemon) shutdownResources(ctx context.Context) {
	d.lifeCancel()
	d.svcWg.Wait()
	if d.metrics != nil {
		d.metrics.shutdown(ctx)
	}
	d.instancesMu.Lock()
	runtimes := make([]*bgpRuntime, 0, len(d.instances))
	for rt := range d.instances {
		runtimes = append(runtimes, rt)
	}
	d.instancesMu.Unlock()
	for _, rt := range runtimes {
		d.closeRuntime(ctx, rt)
	}
}
