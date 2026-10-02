package daemon

import (
	"context"

	apipb "github.com/osrg/gobgp/v3/api"
	log "github.com/sirupsen/logrus"

	"github.com/runityru/anycastd/announcer"
	"github.com/runityru/anycastd/config"
)

// BGPClient is the subset of the GoBGP management API used while
// reconciling configuration generations. *server.BgpServer satisfies it
// and tests substitute a scriptable fake.
type BGPClient interface {
	StartBgp(ctx context.Context, r *apipb.StartBgpRequest) error
	StopBgp(ctx context.Context, r *apipb.StopBgpRequest) error
	AddPeer(ctx context.Context, r *apipb.AddPeerRequest) error
	DeletePeer(ctx context.Context, r *apipb.DeletePeerRequest) error
	UpdatePeer(ctx context.Context, r *apipb.UpdatePeerRequest) (*apipb.UpdatePeerResponse, error)
	WatchEvent(ctx context.Context, r *apipb.WatchEventRequest, fn func(*apipb.WatchEventResponse)) error
}

// PeerMetrics is the Prometheus collector set bound to one BGP
// instance. announcer.Metrics satisfies it.
type PeerMetrics interface {
	Register() error
	Unregister()
	Run(ctx context.Context) error
}

// BGPInstance is one BGP server lifetime, bound to a single global
// router-id / ASN pair. A generation that changes the global BGP
// configuration gets a fresh instance; the previous one is closed only
// after the new generation is committed.
type BGPInstance struct {
	// Client is the management client used for peer reconciliation.
	Client BGPClient
	// Routes is the prefix-level route client bound to this instance.
	Routes announcer.GoBGPServer
	// Serve runs the serving loop. It blocks and is invoked exactly once
	// in its own goroutine when the instance is staged.
	Serve func()
	// NewPeerMetrics builds collector sets labeled for this instance.
	NewPeerMetrics func(routerID string, asn uint32) (PeerMetrics, error)
	// Close stops BGP on the instance.
	Close func(ctx context.Context) error
}

// BGPFactory creates a fresh, not-yet-started BGP instance.
type BGPFactory func(level log.Level) (BGPInstance, error)

// RouteClientFactory builds the dynamic route client through which
// service-driven announcements flow. getActive resolves to the client
// of the currently published generation, which lets a BGP instance
// replacement take effect for every running service atomically.
type RouteClientFactory func(getActive func() BGPClient) announcer.GoBGPServer

// BuildPeer builds a GoBGP peer definition from configuration. The
// local address pins the BGP transport to the configured source
// address.
func BuildPeer(peer config.Peer, localAddress string) *apipb.Peer {
	return &apipb.Peer{
		Conf: &apipb.PeerConf{
			NeighborAddress: peer.RemoteAddress,
			PeerAsn:         peer.RemoteASN,
		},
		EbgpMultihop: &apipb.EbgpMultihop{
			Enabled:     peer.EnableMultihop,
			MultihopTtl: peer.MultihopTTL,
		},
		Transport: &apipb.Transport{
			LocalAddress: localAddress,
		},
	}
}
