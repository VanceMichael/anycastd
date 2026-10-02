package daemon

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/runityru/anycastd/announcer"
)

type bootResult struct {
	d        *Daemon
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan error
	reloadCh chan struct{}
	path     string

	stopOnce sync.Once
	stopErr  error
}

func (b *bootResult) waitStopped(t *testing.T) error {
	t.Helper()
	b.stopOnce.Do(func() {
		select {
		case b.stopErr = <-b.done:
		case <-time.After(3 * time.Second):
			b.stopErr = errors.New("daemon did not stop in time")
		}
	})
	return b.stopErr
}

func boot(t *testing.T, fac *fakeFactory, cfg string) *bootResult {
	t.Helper()
	r := require.New(t)

	dir := t.TempDir()
	path := dir + "/config.yaml"
	r.NoError(writeFile(path, cfg))

	d := New(Options{
		ConfigPath: path,
		AppVersion: "test",
		NewBGP:     fac.newBGP,
		NewRouteClient: func(getActive func() BGPClient) announcer.GoBGPServer {
			return &dynamicRouteClient{getActive: getActive}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	reloadCh := make(chan struct{}, 8)
	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, reloadCh)
	}()

	bootstrapOK := make(chan struct{}, 1)
	go func() {
		if d.ActiveGeneration() == 1 {
			close(bootstrapOK)
			return
		}
		for {
			time.Sleep(5 * time.Millisecond)
			if d.ActiveGeneration() == 1 {
				close(bootstrapOK)
				return
			}
		}
	}()
	select {
	case <-bootstrapOK:
	case err := <-done:
		t.Fatalf("daemon exited during bootstrap: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("daemon did not bootstrap")
	}

	res := &bootResult{d: d, ctx: ctx, cancel: cancel, done: done, reloadCh: reloadCh, path: path}
	t.Cleanup(func() {
		cancel()
		r.NoError(res.waitStopped(t))
	})

	return res
}

func rewrite(t *testing.T, b *bootResult, cfg string) {
	t.Helper()
	require.NoError(t, writeFile(b.path, cfg))
}

func TestBootstrap(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	b := boot(t, fac, renderCfg(baseCfg()))

	fb := fac.instances[0]
	peers, events, _, _ := fb.snapshot()
	r.ElementsMatch([]string{"10.0.0.252"}, peers)
	r.Equal(1, countEvents(events, "add"))

	r.Equal(uint64(1), b.d.ActiveGeneration())
	r.Len(b.d.active.services, 1)

	st := b.d.LastStatus()
	r.True(st.successful)
	r.Equal(reloadApplied, st.result)
	r.Equal(uint64(1), st.generation)
}

func TestNoopReload(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	b := boot(t, fac, renderCfg(baseCfg()))

	fb := fac.instances[0]
	_, eventsBefore, _, _ := fb.snapshot()

	r.NoError(b.d.Reload(context.Background()))
	r.Equal(uint64(1), b.d.ActiveGeneration())
	r.Len(fac.instances, 1)

	_, eventsAfter, _, _ := fb.snapshot()
	r.Len(eventsAfter, len(eventsBefore), "identical config must not touch peers")

	st := b.d.LastStatus()
	r.True(st.successful)
	r.Equal(reloadUnchanged, st.result)

	// The signal path gives the same observable result.
	b.reloadCh <- struct{}{}
	r.Eventually(func() bool {
		return b.d.LastStatus().result == reloadUnchanged
	}, time.Second, 5*time.Millisecond)
}

func TestInvalidCandidateKeepsGeneration(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	b := boot(t, fac, renderCfg(baseCfg()))
	fb := fac.instances[0]

	// Broken YAML: parse/load stage.
	rewrite(t, b, "announcer: : :\n")
	err := b.d.Reload(context.Background())
	r.Error(err)
	r.Equal(uint64(1), b.d.ActiveGeneration())
	st := b.d.LastStatus()
	r.False(st.successful)
	r.Equal(reloadFailed, st.result)

	peers, _, _, _ := fb.snapshot()
	r.ElementsMatch([]string{"10.0.0.252"}, peers)
	r.Len(b.d.active.services, 1)

	// Structurally valid config but unknown checker: validation/build stage.
	bad := `
announcer:
  router_id: 10.0.0.1
  local_address: 10.0.0.1
  local_asn: 65000
  routes:
    - 10.0.0.128/32
  peers:
    - {name: p1, remote_address: 10.0.0.252, remote_asn: 65000}
services:
  - name: web
    check_interval: 5ms
    checks:
      - kind: does-not-exist
        spec:
          x: 1
metrics:
  enabled: true
  address: 127.0.0.1:0
`
	rewrite(t, b, bad)
	r.Error(b.d.Reload(context.Background()))
	r.Equal(uint64(1), b.d.ActiveGeneration())
	r.Len(b.d.active.services, 1)

	// A valid candidate applies normally after the failures.
	cfg2 := baseCfg()
	cfg2.Peers = append(cfg2.Peers, testPeer{Name: "p2", Addr: "10.0.0.253", ASN: 65000})
	rewrite(t, b, renderCfg(cfg2))
	r.NoError(b.d.Reload(context.Background()))
	r.Equal(uint64(2), b.d.ActiveGeneration())
	peers, _, _, _ = fb.snapshot()
	r.ElementsMatch([]string{"10.0.0.252", "10.0.0.253"}, peers)
}

func TestPeerReconcile(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	b := boot(t, fac, renderCfg(baseCfg()))
	fb := fac.instances[0]

	// Modify p1 (multihop), add p2.
	cfg := baseCfg()
	cfg.Peers = []testPeer{
		{Name: "p1", Addr: "10.0.0.252", ASN: 65000, MultiTTL: 3},
		{Name: "p2", Addr: "10.0.0.253", ASN: 65000},
	}
	rewrite(t, b, renderCfg(cfg))
	r.NoError(b.d.Reload(context.Background()))
	r.Equal(uint64(2), b.d.ActiveGeneration())

	peers, _, _, _ := fb.snapshot()
	r.ElementsMatch([]string{"10.0.0.252", "10.0.0.253"}, peers)

	// Remove p1.
	cfg.Peers = []testPeer{{Name: "p2", Addr: "10.0.0.253", ASN: 65000}}
	rewrite(t, b, renderCfg(cfg))
	r.NoError(b.d.Reload(context.Background()))
	r.Equal(uint64(3), b.d.ActiveGeneration())
	peers, _, _, _ = fb.snapshot()
	r.ElementsMatch([]string{"10.0.0.253"}, peers)

	// p1 changes address while p2 stays: identity is the name, address is
	// freed before the new session is added.
	cfg.Peers = []testPeer{
		{Name: "p1", Addr: "10.0.0.250", ASN: 65000},
		{Name: "p2", Addr: "10.0.0.253", ASN: 65000},
	}
	rewrite(t, b, renderCfg(cfg))
	r.NoError(b.d.Reload(context.Background()))
	peers, _, _, _ = fb.snapshot()
	r.ElementsMatch([]string{"10.0.0.250", "10.0.0.253"}, peers)
}

func TestServiceStableIdentity(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	cfg := baseCfg()
	cfg.Services = []testService{
		{Name: "svc-a", Checks: []testCheck{{Tag: "a"}}},
		{Name: "svc-b", Checks: []testCheck{{Tag: "b"}}},
	}
	b := boot(t, fac, renderCfg(cfg))

	checkerID := func(name string) uint64 {
		return b.d.active.prepared[name].checks[0].Check.(*testChecker).id
	}
	servicePtr := func(name string) *managedService {
		return b.d.active.services[name]
	}

	idA1 := checkerID("svc-a")
	idB1 := checkerID("svc-b")
	svcA1 := servicePtr("svc-a")
	svcB1 := servicePtr("svc-b")

	// svc-a unchanged, svc-b check spec changes, svc-c added.
	cfg.Services = []testService{
		{Name: "svc-a", Checks: []testCheck{{Tag: "a"}}},
		{Name: "svc-b", Checks: []testCheck{{Tag: "b2"}}},
		{Name: "svc-c", Checks: []testCheck{{Tag: "c"}}},
	}
	rewrite(t, b, renderCfg(cfg))
	r.NoError(b.d.Reload(context.Background()))

	r.Equal(idA1, checkerID("svc-a"), "unchanged service must keep its checker")
	r.NotEqual(idB1, checkerID("svc-b"), "changed service must get a new checker")
	r.Equal(svcA1, servicePtr("svc-a"), "unchanged service goroutine must survive")
	r.NotEqual(svcB1, servicePtr("svc-b"))
	r.Contains(b.d.active.services, "svc-c")
}

func TestRollbackOnPeerFailureAndRetry(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	b := boot(t, fac, renderCfg(baseCfg()))

	var failOnce sync.Map // addr -> remaining failures
	fac.hook = func(_ context.Context, _ *fakeBGP, op, addr string) error {
		if op == "add" {
			if v, ok := failOnce.Load(addr); ok {
				if v.(*atomic.Int32).Swap(0) > 0 {
					return errors.New("simulated peer add failure")
				}
			}
		}
		return nil
	}

	cnt := &atomic.Int32{}
	cnt.Store(1)
	failOnce.Store("10.0.1.2", cnt)

	cfg := baseCfg()
	cfg.Peers = []testPeer{
		{Name: "p1", Addr: "10.0.0.252", ASN: 65000},
		{Name: "pa", Addr: "10.0.1.1", ASN: 65000},
		{Name: "pb", Addr: "10.0.1.2", ASN: 65000},
	}
	rewrite(t, b, renderCfg(cfg))

	err := b.d.Reload(context.Background())
	r.Error(err)
	r.Equal(uint64(1), b.d.ActiveGeneration(), "failed candidate must not be published")

	fb := fac.instances[0]
	peers, events, _, _ := fb.snapshot()
	r.ElementsMatch([]string{"10.0.0.252"}, peers, "compensation must remove the peer added before the failure")
	r.Equal(1, countEvents(events, "add:10.0.1.1"))
	r.Equal(1, countEvents(events, "delete:10.0.1.1"))
	r.Equal(0, countEvents(events, "add:10.0.1.2"))
	r.Len(b.d.active.services, 1)
	r.Len(fac.instances, 1, "BGP instance must not be recreated for an unchanged global config")

	st := b.d.LastStatus()
	r.False(st.successful)

	// Fault gone: the same configuration reloads successfully.
	r.NoError(b.d.Reload(context.Background()))
	r.Equal(uint64(2), b.d.ActiveGeneration())
	peers, _, _, _ = fb.snapshot()
	r.ElementsMatch([]string{"10.0.0.252", "10.0.1.1", "10.0.1.2"}, peers)
}

func TestRouteAggregationAndReconcile(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	cfg := baseCfg()
	cfg.Services = []testService{
		{Name: "svc-a", Interval: "2ms", Checks: []testCheck{{Tag: "a"}}},
		{Name: "svc-b", Interval: "2ms", Checks: []testCheck{{Tag: "b"}}},
	}
	b := boot(t, fac, renderCfg(cfg))
	fb := fac.instances[0]

	// Both services up: each prefix announced exactly once.
	r.Eventually(func() bool {
		paths, events := fb.route.snapshot()
		return len(paths) == 1 && countEvents(events, "add:10.0.0.128/32") == 1
	}, 2*time.Second, 2*time.Millisecond)

	// Remove svc-b: svc-a stays up, no route churn.
	_, eventsBefore := fb.route.snapshot()
	cfg.Services = []testService{{Name: "svc-a", Interval: "2ms", Checks: []testCheck{{Tag: "a"}}}}
	rewrite(t, b, renderCfg(cfg))
	r.NoError(b.d.Reload(context.Background()))

	r.Eventually(func() bool {
		_, events := fb.route.snapshot()
		return len(events) == len(eventsBefore)
	}, time.Second, 2*time.Millisecond)
	paths, _ := fb.route.snapshot()
	r.Contains(paths, "10.0.0.128/32")

	// Replace the healthy service by a failing one: routes withdrawn.
	cfg.Services = []testService{{Name: "svc-x", Interval: "2ms", Checks: []testCheck{{Tag: "x", Fail: true}}}}
	rewrite(t, b, renderCfg(cfg))
	r.NoError(b.d.Reload(context.Background()))
	r.Eventually(func() bool {
		paths, _ := fb.route.snapshot()
		return len(paths) == 0
	}, 2*time.Second, 2*time.Millisecond)

	// Healthy service again with a second prefix: both get announced.
	cfg.Routes = []string{"10.0.0.128/32", "10.0.0.129/32"}
	cfg.Services = []testService{{Name: "svc-x", Interval: "2ms", Checks: []testCheck{{Tag: "x"}}}}
	rewrite(t, b, renderCfg(cfg))
	r.NoError(b.d.Reload(context.Background()))
	r.Eventually(func() bool {
		paths, _ := fb.route.snapshot()
		return len(paths) == 2
	}, 2*time.Second, 2*time.Millisecond)
	paths, _ = fb.route.snapshot()
	r.Contains(paths, "10.0.0.128/32")
	r.Contains(paths, "10.0.0.129/32")
}

func TestConcurrentReloadsAreSerialized(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	b := boot(t, fac, renderCfg(baseCfg()))

	cfg := baseCfg()
	cfg.Peers = append(cfg.Peers, testPeer{Name: "pc", Addr: "10.0.0.254", ASN: 65000})
	rewrite(t, b, renderCfg(cfg))

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.d.Reload(context.Background())
		}()
	}
	wg.Wait()

	r.Equal(uint64(2), b.d.ActiveGeneration())
	fb := fac.instances[0]
	_, events, _, _ := fb.snapshot()
	r.Equal(1, countEvents(events, "add:10.0.0.254"), "no duplicate peers under concurrent reloads")

	// Coalesced SIGHUPs through the signal channel produce one apply.
	cfg.Peers = append(cfg.Peers, testPeer{Name: "pd", Addr: "10.0.0.251", ASN: 65000})
	rewrite(t, b, renderCfg(cfg))
	b.reloadCh <- struct{}{}
	b.reloadCh <- struct{}{}
	r.Eventually(func() bool {
		return b.d.ActiveGeneration() == 3
	}, 2*time.Second, 5*time.Millisecond)
	_, events, _, _ = fb.snapshot()
	r.Equal(1, countEvents(events, "add:10.0.0.251"))
}

func TestShutdownDuringReloadCancelsAndCompensates(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	b := boot(t, fac, renderCfg(baseCfg()))

	entered := make(chan struct{}, 1)
	fac.hook = func(ctx context.Context, fb *fakeBGP, op, addr string) error {
		if fb.id == 1 && op == "add" && addr == "10.0.9.9" {
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}

	// Global change: a new BGP instance is staged; a peer add blocks until
	// shutdown cancels the reload.
	cfg := baseCfg()
	cfg.RouterID = "10.0.0.2"
	cfg.Peers = append(cfg.Peers, testPeer{Name: "pblock", Addr: "10.0.9.9", ASN: 65000})
	rewrite(t, b, renderCfg(cfg))

	relErr := make(chan error, 1)
	go func() {
		relErr <- b.d.Reload(b.ctx)
	}()
	<-entered

	b.cancel()

	select {
	case err := <-relErr:
		r.Error(err, "canceled reload must report its error")
	case <-time.After(3 * time.Second):
		t.Fatal("reload did not return after shutdown")
	}

	r.NoError(b.waitStopped(t), "daemon must stop exactly once through the normal shutdown path")

	r.Len(fac.instances, 2)
	for i, fb := range fac.instances {
		_, _, closed, closeCount := fb.snapshot()
		r.True(closed, "instance %d must be closed", i)
		r.Equal(1, closeCount, "instance %d must be closed exactly once", i)
	}

	// The old generation never published the candidate.
	r.Equal(uint64(1), b.d.ActiveGeneration())
}

func gatherMetric(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

func TestReloadMetricsExposure(t *testing.T) {
	r := require.New(t)
	fac := &fakeFactory{}
	b := boot(t, fac, renderCfg(baseCfg()))

	r.Eventually(func() bool {
		return gatherMetric(t, "anycastd_config_generation") != nil
	}, time.Second, 5*time.Millisecond)

	mf := gatherMetric(t, "anycastd_config_generation")
	r.Equal(1.0, mf.Metric[0].Gauge.GetValue())

	// Successful generation bump.
	cfg := baseCfg()
	cfg.Peers = append(cfg.Peers, testPeer{Name: "p2", Addr: "10.0.0.253", ASN: 65000})
	rewrite(t, b, renderCfg(cfg))
	r.NoError(b.d.Reload(context.Background()))

	r.Eventually(func() bool {
		return gatherMetric(t, "anycastd_config_generation").Metric[0].Gauge.GetValue() == 2
	}, time.Second, 5*time.Millisecond)

	// Failed reload: generation stays, success gauge 0, reason exposed.
	rewrite(t, b, "not: a: valid: yaml\n")
	r.Error(b.d.Reload(context.Background()))
	r.Equal(uint64(2), b.d.ActiveGeneration())

	succ := gatherMetric(t, "anycastd_config_last_reload_successful")
	r.Equal(0.0, succ.Metric[0].Gauge.GetValue())
	info := gatherMetric(t, "anycastd_config_last_reload_info")
	var foundFailure bool
	for _, m := range info.Metric {
		for _, l := range m.Label {
			if l.GetName() == "result" && l.GetValue() == reloadFailed && m.Gauge.GetValue() == 1 {
				foundFailure = true
			}
		}
	}
	r.True(foundFailure)

	// BGP instance replacement swaps the peer collector set exactly.
	cfg2 := baseCfg()
	cfg2.RouterID = "10.0.0.9"
	cfg2.Peers = []testPeer{{Name: "p1", Addr: "10.0.0.252", ASN: 65000}}
	rewrite(t, b, renderCfg(cfg2))
	r.NoError(b.d.Reload(context.Background()))

	r.Len(fac.instances, 2)
	oldSets := fac.instances[0].peerMetricsSnapshot()
	newSets := fac.instances[1].peerMetricsSnapshot()
	r.Len(oldSets, 1)
	r.Len(newSets, 1)
	r.Equal(int32(1), oldSets[0].registered.Load())
	r.Equal(int32(1), oldSets[0].unregistered.Load())
	r.Equal(int32(1), newSets[0].registered.Load())
	r.Equal(int32(0), newSets[0].unregistered.Load())
}
