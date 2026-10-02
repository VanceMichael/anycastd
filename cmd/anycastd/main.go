package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/kelseyhightower/envconfig"
	apipb "github.com/osrg/gobgp/v3/api"
	"github.com/osrg/gobgp/v3/pkg/server"
	log "github.com/sirupsen/logrus"

	"github.com/runityru/anycastd/announcer"
	"github.com/runityru/anycastd/announcer/gobgpv3"
	"github.com/runityru/anycastd/config"
	"github.com/runityru/anycastd/daemon"

	// checkers in build
	_ "github.com/runityru/anycastd/checkers/assigned_address"
	_ "github.com/runityru/anycastd/checkers/dns_lookup"
	_ "github.com/runityru/anycastd/checkers/http_2xx"
	_ "github.com/runityru/anycastd/checkers/icmp_ping"
	_ "github.com/runityru/anycastd/checkers/ntpq"
	_ "github.com/runityru/anycastd/checkers/tftp_rrq"
	_ "github.com/runityru/anycastd/checkers/tls_certificate"
)

var (
	appVersion     = "n/a (dev build)"
	buildTimestamp = "undefined"
)

// newPeer builds a gobgp peer definition. localAddress is set as the
// transport local address so the BGP session is sourced from the
// configured address instead of whatever the kernel picks by route
// lookup.
func newPeer(peer config.Peer, localAddress string) *apipb.Peer {
	return daemon.BuildPeer(peer, localAddress)
}

type spec struct {
	ConfigPath string    `envconfig:"CONFIG_PATH" default:"/config.yaml"`
	LogLevel   log.Level `envconfig:"LOG_LEVEL" default:"WARN"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := spec{}
	envconfig.MustProcess("", &s)

	log.SetLevel(s.LogLevel)

	lf := new(log.TextFormatter)
	lf.FullTimestamp = true
	log.SetFormatter(lf)

	log.Infof("Initializing anycastd (%s @ %s) ...", appVersion, buildTimestamp)

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)

	// SIGHUPs are coalesced: while a reload is in progress further
	// signals collapse into at most one follow-up reload.
	reloadCh := make(chan struct{}, 1)
	go func() {
		for range hupCh {
			select {
			case reloadCh <- struct{}{}:
			default:
			}
		}
	}()

	d := daemon.New(daemon.Options{
		ConfigPath: s.ConfigPath,
		LogLevel:   s.LogLevel,
		AppVersion: appVersion,
		NewBGP: func(level log.Level) (daemon.BGPInstance, error) {
			bgpSrv := server.NewBgpServer(server.LoggerOption(&announcer.Logger{
				Logger: &log.Logger{
					Out:       os.Stderr,
					Formatter: lf,
					Hooks:     make(log.LevelHooks),
					Level:     level,
				},
			}))

			return daemon.BGPInstance{
				Client: bgpSrv,
				Routes: gobgpv3.NewAdapter(bgpSrv),
				Serve:  bgpSrv.Serve,
				Close: func(ctx context.Context) error {
					return bgpSrv.StopBgp(ctx, &apipb.StopBgpRequest{})
				},
				NewPeerMetrics: func(routerID string, asn uint32) (daemon.PeerMetrics, error) {
					return announcer.NewMetricsRepository(bgpSrv, routerID, asn), nil
				},
			}, nil
		},
		NewRouteClient: func(getActive func() daemon.BGPClient) announcer.GoBGPServer {
			return gobgpv3.NewDynamicAdapter(func() *server.BgpServer {
				c := getActive()
				if c == nil {
					return nil
				}
				return c.(*server.BgpServer)
			})
		},
	})

	if err := d.Run(ctx, reloadCh); err != nil {
		log.Fatal(err)
	}
}
