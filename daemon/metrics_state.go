package daemon

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/runityru/anycastd/service"
)

// Reload result labels exposed via Prometheus.
const (
	reloadApplied   = "applied"
	reloadUnchanged = "unchanged"
	reloadFailed    = "failed"
)

// reloadStatus is the observable state of the configuration generation
// machinery. It is kept in memory even while the metrics endpoint is
// disabled and pushed to collectors whenever it is enabled.
type reloadStatus struct {
	generation uint64
	successful bool
	timestamp  time.Time
	result     string
	reason     string
}

// reloadMetrics exposes the currently effective generation and the
// outcome of the most recent reload attempt. A failed candidate never
// changes the generation gauge; only the last-reload gauges describe
// the failure.
type reloadMetrics struct {
	generation *prometheus.GaugeVec
	successful prometheus.Gauge
	timestamp  prometheus.Gauge
	lastResult *prometheus.GaugeVec
}

func newReloadMetrics() *reloadMetrics {
	return &reloadMetrics{
		generation: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "config_generation",
			Help:      "Sequence number of the currently effective configuration generation",
		}, nil),
		successful: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "config_last_reload_successful",
			Help:      "1 if the most recent configuration reload completed successfully, 0 otherwise",
		}),
		timestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "config_last_reload_timestamp_seconds",
			Help:      "Unix timestamp of the most recent configuration reload attempt",
		}),
		lastResult: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "config_last_reload_info",
			Help:      "Information about the most recent configuration reload; value is 1 for the active result/reason pair",
		}, []string{"result", "reason"}),
	}
}

func (m *reloadMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.generation, m.successful, m.timestamp, m.lastResult}
}

func (m *reloadMetrics) register() error {
	for _, c := range m.collectors() {
		if err := prometheus.Register(c); err != nil {
			return err
		}
	}
	return nil
}

func (m *reloadMetrics) unregister() {
	for _, c := range m.collectors() {
		prometheus.Unregister(c)
	}
}

func (m *reloadMetrics) publish(st reloadStatus) {
	m.generation.Reset()
	m.generation.WithLabelValues().Set(float64(st.generation))
	if st.successful {
		m.successful.Set(1)
	} else {
		m.successful.Set(0)
	}
	m.timestamp.Set(float64(st.timestamp.UnixNano()) / 1e9)
	m.lastResult.Reset()
	m.lastResult.WithLabelValues(st.result, st.reason).Set(1)
}

// metricsProxy is the permanent service.Metrics view held by every
// service. Toggling the metrics endpoint across generations rebinds
// the underlying implementation (or clears it) without restarting any
// service or check.
type metricsProxy struct {
	p atomic.Pointer[service.Metrics]
}

func newMetricsProxy() *metricsProxy {
	return &metricsProxy{}
}

var _ service.Metrics = (*metricsProxy)(nil)

func (mp *metricsProxy) set(m service.Metrics) {
	if m == nil {
		mp.p.Store(nil)
		return
	}
	v := m
	mp.p.Store(&v)
}

func (mp *metricsProxy) current() service.Metrics {
	if v := mp.p.Load(); v != nil {
		return *v
	}
	return nil
}

func (mp *metricsProxy) ServiceUp(svc string) {
	if m := mp.current(); m != nil {
		m.ServiceUp(svc)
	}
}

func (mp *metricsProxy) ServiceDown(svc string) {
	if m := mp.current(); m != nil {
		m.ServiceDown(svc)
	}
}

func (mp *metricsProxy) DeleteService(svc string) {
	if m := mp.current(); m != nil {
		m.DeleteService(svc)
	}
}

func (mp *metricsProxy) Unregister() {}

func (mp *metricsProxy) MeasureCall(ctx context.Context, svc, check string, fn func(ctx context.Context) error) error {
	if m := mp.current(); m != nil {
		return m.MeasureCall(ctx, svc, check, fn)
	}
	return fn(ctx)
}

// httpRuntime is one live metrics HTTP endpoint.
type httpRuntime struct {
	server   *http.Server
	listener net.Listener
}

func startMetricsHTTP(address string) (*httpRuntime, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, errors.Wrap(err, "error listening on metrics address")
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Handler: mux}

	go func() {
		_ = srv.Serve(lis)
	}()

	return &httpRuntime{server: srv, listener: lis}, nil
}

func (h *httpRuntime) shutdown(ctx context.Context) {
	if h == nil || h.server == nil {
		return
	}
	_ = h.server.Shutdown(ctx)
}

// metricsSupervisor owns the metrics side of generation switches: the
// per-process service collectors, the BGP-scraping collectors (replaced
// together with their BGP instance), the reload-state collectors and
// the metrics HTTP endpoint. No collector is registered twice and
// unchanged resources are never touched.
type metricsSupervisor struct {
	appVersion string
	baseCtx    context.Context
	proxy      *metricsProxy

	mu sync.Mutex

	enabled  bool
	address  string
	routerID string
	asn      uint32

	http *httpRuntime

	serviceMetrics service.Metrics
	peerMetrics    PeerMetrics
	peerCancel     context.CancelFunc
	peerDone       chan struct{}

	reload *reloadMetrics
}

func newMetricsSupervisor(appVersion string, baseCtx context.Context, proxy *metricsProxy) *metricsSupervisor {
	return &metricsSupervisor{
		appVersion: appVersion,
		baseCtx:    baseCtx,
		proxy:      proxy,
	}
}

func (s *metricsSupervisor) publishStatus(st reloadStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reload != nil {
		s.reload.publish(st)
	}
}

func (s *metricsSupervisor) deleteServiceLabels(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serviceMetrics != nil {
		s.serviceMetrics.DeleteService(name)
	}
}

// peerMetricsFactory builds collector sets for the target BGP instance
// of a generation switch.
type peerMetricsFactory func() (PeerMetrics, error)

// metricsView is the metrics portion of a candidate generation plus
// the reload status displayed while a transition is in flight.
type metricsView struct {
	Enabled       bool
	Address       string
	InitialStatus reloadStatus
}

// metricsTxn is one reconciled metrics transition. stage performs only
// additive, externally invisible work (collector registration without
// a serving endpoint, listener binding) and is fully reversible;
// commit performs the visibility swap (HTTP serving, proxy binding,
// BGP collector replacement, teardown on disable).
type metricsTxn struct {
	stage    func(ctx context.Context) error
	commit   func(ctx context.Context) error
	rollback func(ctx context.Context)
}

func (s *metricsSupervisor) plan(desired metricsView, routerID string, asn uint32, newPeer peerMetricsFactory) metricsTxn {
	s.mu.Lock()
	curEnabled := s.enabled
	curAddress := s.address
	curRouterID := s.routerID
	curASN := s.asn
	s.mu.Unlock()

	enable := !curEnabled && desired.Enabled
	disable := curEnabled && !desired.Enabled
	addrChange := curEnabled && desired.Enabled && curAddress != desired.Address
	bgpKeyChange := curEnabled && desired.Enabled && (curRouterID != routerID || curASN != asn)

	var (
		stagedSM     service.Metrics
		stagedRM     *reloadMetrics
		stagedPM     PeerMetrics // registered collectors for an enable transition
		stagedHTTP   *httpRuntime
		swapPM       PeerMetrics // unregistered, for a BGP instance replacement
		enablePrimed bool
	)

	stage := func(ctx context.Context) error {
		if enable {
			sm, err := service.NewMetrics(s.appVersion)
			if err != nil {
				return err
			}

			rm := newReloadMetrics()
			rm.publish(desired.InitialStatus)
			if err := rm.register(); err != nil {
				sm.Unregister()
				return err
			}

			pm, err := newPeer()
			if err != nil {
				rm.unregister()
				sm.Unregister()
				return err
			}
			if err := pm.Register(); err != nil {
				rm.unregister()
				sm.Unregister()
				return err
			}

			// Bind the listener early so a busy port fails before the
			// generation is committed, but do not serve it yet.
			lis, err := net.Listen("tcp", desired.Address)
			if err != nil {
				pm.Unregister()
				rm.unregister()
				sm.Unregister()
				return errors.Wrap(err, "error listening on metrics address")
			}

			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())
			stagedSM = sm
			stagedRM = rm
			stagedPM = pm
			stagedHTTP = &httpRuntime{server: &http.Server{Handler: mux}, listener: lis}
			enablePrimed = true
			return nil
		}

		if addrChange {
			rt, err := startMetricsHTTP(desired.Address)
			if err != nil {
				return err
			}
			stagedHTTP = rt
			return nil
		}

		if bgpKeyChange {
			pm, err := newPeer()
			if err != nil {
				return err
			}
			swapPM = pm
		}

		return nil
	}

	commit := func(ctx context.Context) error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Replace BGP-bound collectors first: failure restores the old
		// set and aborts the whole generation.
		if bgpKeyChange && swapPM != nil {
			if err := s.swapPeerMetricsLocked(ctx, swapPM); err != nil {
				return err
			}
			s.routerID = routerID
			s.asn = asn
		}

		if disable {
			// Hide the endpoint from services and clients first.
			s.proxy.set(nil)
			s.teardownLocked(ctx)
			s.enabled = false
			s.address = ""
			s.routerID = ""
			s.asn = 0
			return nil
		}

		if enablePrimed {
			go func() {
				_ = stagedHTTP.server.Serve(stagedHTTP.listener)
			}()
			s.startPeerLoopLocked(stagedPM)
			s.serviceMetrics = stagedSM
			s.peerMetrics = stagedPM
			s.reload = stagedRM
			s.http = stagedHTTP
			s.proxy.set(stagedSM)
			s.enabled = true
			s.address = desired.Address
			s.routerID = routerID
			s.asn = asn
			return nil
		}

		if addrChange && stagedHTTP != nil {
			if s.http != nil {
				s.http.shutdown(ctx)
			}
			s.http = stagedHTTP
			s.address = desired.Address
		}

		return nil
	}

	rollback := func(ctx context.Context) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if stagedHTTP != nil {
			if enablePrimed {
				// Never served: close the listener and drop collectors.
				_ = stagedHTTP.listener.Close()
			} else {
				stagedHTTP.shutdown(ctx)
			}
		}

		if enablePrimed {
			if stagedPM != nil {
				stagedPM.Unregister()
			}
			if stagedRM != nil {
				stagedRM.unregister()
			}
			if stagedSM != nil {
				stagedSM.Unregister()
			}
			// Proxy was never bound to the staged implementation.
		}
	}

	return metricsTxn{stage: stage, commit: commit, rollback: rollback}
}

// startPeerLoopLocked starts the periodic BGP scrape loop for a
// collector set. Callers must hold s.mu.
func (s *metricsSupervisor) startPeerLoopLocked(pm PeerMetrics) {
	ctx, cancel := context.WithCancel(s.baseCtx)
	done := make(chan struct{})
	s.peerCancel = cancel
	s.peerDone = done
	go func() {
		_ = pm.Run(ctx)
		close(done)
	}()
}

func (s *metricsSupervisor) stopPeerLoopLocked(ctx context.Context) {
	if s.peerCancel == nil {
		return
	}
	cancel := s.peerCancel
	done := s.peerDone
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
	}
	s.peerCancel = nil
	s.peerDone = nil
}

// swapPeerMetricsLocked replaces the BGP-bound collectors. The old
// collectors are unregistered before the new ones are registered; if
// registration of the new set fails, the old set is restored so the
// daemon keeps exposing the previous generation. Callers must hold
// s.mu.
func (s *metricsSupervisor) swapPeerMetricsLocked(ctx context.Context, pm PeerMetrics) error {
	old := s.peerMetrics
	if old != nil {
		s.stopPeerLoopLocked(ctx)
		old.Unregister()
	}

	if err := pm.Register(); err != nil {
		if old != nil {
			if rerr := old.Register(); rerr == nil {
				s.startPeerLoopLocked(old)
			}
		}
		s.peerMetrics = old
		return errors.Wrap(err, "error registering BGP metrics for new generation")
	}

	s.peerMetrics = pm
	s.startPeerLoopLocked(pm)
	return nil
}

// teardownLocked stops the HTTP endpoint, the scrape loop and
// unregisters every collector owned by the supervisor. Callers must
// hold s.mu.
func (s *metricsSupervisor) teardownLocked(ctx context.Context) {
	if s.http != nil {
		s.http.shutdown(ctx)
		s.http = nil
	}
	s.stopPeerLoopLocked(ctx)
	if s.peerMetrics != nil {
		s.peerMetrics.Unregister()
		s.peerMetrics = nil
	}
	if s.reload != nil {
		s.reload.unregister()
		s.reload = nil
	}
	if s.serviceMetrics != nil {
		s.serviceMetrics.Unregister()
		s.serviceMetrics = nil
	}
}

// shutdown stops the metrics endpoint and all scrape loops during
// daemon termination.
func (s *metricsSupervisor) shutdown(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.teardownLocked(ctx)
}
