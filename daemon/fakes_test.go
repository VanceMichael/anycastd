package daemon

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/runityru/anycastd/announcer"
	"github.com/runityru/anycastd/checkers"
	apipb "github.com/osrg/gobgp/v3/api"
)

// ---------------------------------------------------------------------------
// test checker
// ---------------------------------------------------------------------------

type testChecker struct {
	id   uint64
	tag  string
	fail bool
}

var (
	testCheckerSeq uint64
	testCheckerReg sync.Once
)

func (c *testChecker) Kind() string { return "daemontest" }

func (c *testChecker) Check(context.Context) error {
	if c.fail {
		return errors.New("checker forced to fail")
	}
	return nil
}

func registerTestChecker() {
	testCheckerReg.Do(func() {
		checkers.MustRegister("daemontest", func(raw json.RawMessage) (checkers.Checker, error) {
			var spec struct {
				Tag  string `json:"tag"`
				Fail bool   `json:"fail"`
			}
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &spec); err != nil {
					return nil, err
				}
			}
			return &testChecker{
				id:   atomic.AddUint64(&testCheckerSeq, 1),
				tag:  spec.Tag,
				fail: spec.Fail,
			}, nil
		})
	})
}

// ---------------------------------------------------------------------------
// fake BGP
// ---------------------------------------------------------------------------

type bgpHook func(ctx context.Context, fb *fakeBGP, op, addr string) error

type fakeFactory struct {
	mu        sync.Mutex
	instances []*fakeBGP
	hook      bgpHook
	routeHook func(ctx context.Context, fb *fakeBGP, op, prefix string) error
}

type fakeBGP struct {
	fac *fakeFactory
	id  int

	mu      sync.Mutex
	started bool
	closed  bool

	closeCount int

	// peers keyed by neighbor address
	peers map[string]bool

	events []string

	route *fakeRouteClient

	peerMetricsSets []*fakePeerMetrics
}

var _ BGPClient = (*fakeBGP)(nil)

func (f *fakeBGP) logEvent(ev string) {
	f.mu.Lock()
	f.events = append(f.events, ev)
	f.mu.Unlock()
}

func (f *fakeBGP) runHook(ctx context.Context, op, addr string) error {
	if f.fac.hook == nil {
		return nil
	}
	return f.fac.hook(ctx, f, op, addr)
}

func (f *fakeBGP) StartBgp(ctx context.Context, _ *apipb.StartBgpRequest) error {
	if err := f.runHook(ctx, "start", ""); err != nil {
		return err
	}
	f.mu.Lock()
	f.started = true
	f.mu.Unlock()
	return nil
}

func (f *fakeBGP) StopBgp(context.Context, *apipb.StopBgpRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	f.closeCount++
	return nil
}

func (f *fakeBGP) AddPeer(ctx context.Context, r *apipb.AddPeerRequest) error {
	addr := r.GetPeer().GetConf().GetNeighborAddress()
	if err := f.runHook(ctx, "add", addr); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peers[addr] {
		return errors.Errorf("peer %s already exists", addr)
	}
	f.peers[addr] = true
	f.events = append(f.events, "add:"+addr)
	return nil
}

func (f *fakeBGP) DeletePeer(ctx context.Context, r *apipb.DeletePeerRequest) error {
	addr := r.Address
	if err := f.runHook(ctx, "delete", addr); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.peers[addr] {
		return errors.Errorf("peer %s does not exist", addr)
	}
	delete(f.peers, addr)
	f.events = append(f.events, "delete:"+addr)
	return nil
}

func (f *fakeBGP) UpdatePeer(context.Context, *apipb.UpdatePeerRequest) (*apipb.UpdatePeerResponse, error) {
	return &apipb.UpdatePeerResponse{}, nil
}

func (f *fakeBGP) WatchEvent(ctx context.Context, _ *apipb.WatchEventRequest, _ func(*apipb.WatchEventResponse)) error {
	<-ctx.Done()
	return nil
}

func (f *fakeBGP) snapshot() (peers []string, events []string, closed bool, closeCount int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for addr := range f.peers {
		peers = append(peers, addr)
	}
	events = append(events, f.events...)
	return peers, events, f.closed, f.closeCount
}

func (fac *fakeFactory) newBGP(_ log.Level) (BGPInstance, error) {
	fac.mu.Lock()
	id := len(fac.instances)
	fb := &fakeBGP{
		fac:   fac,
		id:    id,
		peers: map[string]bool{},
		route: newFakeRouteClient(fac, id),
	}
	fac.instances = append(fac.instances, fb)
	fac.mu.Unlock()

	return BGPInstance{
		Client: fb,
		Routes: fb.route,
		Serve:  func() {},
		Close: func(ctx context.Context) error {
			return fb.StopBgp(ctx, &apipb.StopBgpRequest{})
		},
		NewPeerMetrics: func(_ string, _ uint32) (PeerMetrics, error) {
			pm := &fakePeerMetrics{}
			fb.mu.Lock()
			fb.peerMetricsSets = append(fb.peerMetricsSets, pm)
			fb.mu.Unlock()
			return pm, nil
		},
	}, nil
}

func (f *fakeBGP) peerMetricsSnapshot() []*fakePeerMetrics {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*fakePeerMetrics{}, f.peerMetricsSets...)
}

func (fac *fakeFactory) active() *fakeBGP {
	fac.mu.Lock()
	defer fac.mu.Unlock()
	return fac.instances[len(fac.instances)-1]
}

// ---------------------------------------------------------------------------
// fake route client
// ---------------------------------------------------------------------------

type fakeRouteClient struct {
	fac *fakeFactory
	id  int

	mu     sync.Mutex
	paths  map[string]string // prefix -> next hop
	events []string
}

var _ announcer.GoBGPServer = (*fakeRouteClient)(nil)

func newFakeRouteClient(fac *fakeFactory, id int) *fakeRouteClient {
	return &fakeRouteClient{fac: fac, id: id, paths: map[string]string{}}
}

func (r *fakeRouteClient) AddPath(ctx context.Context, prefix, nextHop string) error {
	if r.fac.routeHook != nil {
		if err := r.fac.routeHook(ctx, r.fac.instances[r.id], "add", prefix); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths[prefix] = nextHop
	r.events = append(r.events, "add:"+prefix)
	return nil
}

func (r *fakeRouteClient) DeletePath(ctx context.Context, prefix string) error {
	if r.fac.routeHook != nil {
		if err := r.fac.routeHook(ctx, r.fac.instances[r.id], "delete", prefix); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.paths, prefix)
	r.events = append(r.events, "delete:"+prefix)
	return nil
}

func (r *fakeRouteClient) snapshot() (map[string]string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	paths := map[string]string{}
	for k, v := range r.paths {
		paths[k] = v
	}
	return paths, append([]string(nil), r.events...)
}

// dynamicRouteClient follows the active generation, like gobgpv3's
// dynamic adapter.
type dynamicRouteClient struct {
	getActive func() BGPClient
}

var _ announcer.GoBGPServer = (*dynamicRouteClient)(nil)

func (c *dynamicRouteClient) current() *fakeRouteClient {
	client := c.getActive()
	if client == nil {
		return nil
	}
	return client.(*fakeBGP).route
}

func (c *dynamicRouteClient) AddPath(ctx context.Context, prefix, nextHop string) error {
	return c.current().AddPath(ctx, prefix, nextHop)
}

func (c *dynamicRouteClient) DeletePath(ctx context.Context, prefix string) error {
	return c.current().DeletePath(ctx, prefix)
}

// ---------------------------------------------------------------------------
// fake peer metrics
// ---------------------------------------------------------------------------

type fakePeerMetrics struct {
	registered   atomic.Int32
	unregistered atomic.Int32
	runs         atomic.Int32
}

var _ PeerMetrics = (*fakePeerMetrics)(nil)

func (m *fakePeerMetrics) Register() error {
	m.registered.Add(1)
	return nil
}

func (m *fakePeerMetrics) Unregister() {
	m.unregistered.Add(1)
}

func (m *fakePeerMetrics) Run(ctx context.Context) error {
	m.runs.Add(1)
	<-ctx.Done()
	return nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newTestDaemon(t *testing.T, fac *fakeFactory, cfg string) (*Daemon, context.CancelFunc, chan error) {
	t.Helper()
	require := require.New(t)

	dir := t.TempDir()
	path := dir + "/config.yaml"
	require.NoError(writeFile(path, cfg))

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

	require.Eventually(func() bool {
		return d.ActiveGeneration() == 1
	}, 3_000_000_000, 5_000_000, "daemon did not bootstrap")

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(err)
		}
	})

	return d, cancel, done
}
