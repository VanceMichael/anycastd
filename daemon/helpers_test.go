package daemon

import (
	"fmt"
	"os"
	"strings"
)

func init() {
	registerTestChecker()
}

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

type testPeer struct {
	Name     string
	Addr     string
	ASN      uint32
	MultiTTL uint32
}

type testCheck struct {
	Tag  string
	Fail bool
}

type testService struct {
	Name     string
	Interval string
	Checks   []testCheck
}

type testConfig struct {
	RouterID    string
	LocalAddr   string
	LocalASN    uint32
	Routes      []string
	Peers       []testPeer
	Services    []testService
	MetricsAddr string
}

func renderCfg(c testConfig) string {
	if c.LocalAddr == "" {
		c.LocalAddr = "10.0.0.1"
	}
	if c.LocalASN == 0 {
		c.LocalASN = 65000
	}
	if c.MetricsAddr == "" {
		c.MetricsAddr = "127.0.0.1:0"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "announcer:\n  router_id: %s\n  local_address: %s\n  local_asn: %d\n", c.RouterID, c.LocalAddr, c.LocalASN)
	b.WriteString("  routes:\n")
	for _, r := range c.Routes {
		fmt.Fprintf(&b, "    - %s\n", r)
	}
	b.WriteString("  peers:\n")
	for _, p := range c.Peers {
		fmt.Fprintf(&b, "    - name: %s\n      remote_address: %s\n      remote_asn: %d\n", p.Name, p.Addr, p.ASN)
		if p.MultiTTL > 0 {
			fmt.Fprintf(&b, "      enable_multihop: true\n      multihop_ttl: %d\n", p.MultiTTL)
		}
	}
	b.WriteString("services:\n")
	for _, s := range c.Services {
		interval := s.Interval
		if interval == "" {
			interval = "5ms"
		}
		fmt.Fprintf(&b, "  - name: %s\n    check_interval: %s\n    checks:\n", s.Name, interval)
		for _, ck := range s.Checks {
			b.WriteString("      - kind: daemontest\n        spec:\n")
			fmt.Fprintf(&b, "          tag: %s\n", ck.Tag)
			if ck.Fail {
				b.WriteString("          fail: true\n")
			}
		}
	}
	fmt.Fprintf(&b, "metrics:\n  enabled: true\n  address: %s\n", c.MetricsAddr)
	return b.String()
}

func baseCfg() testConfig {
	return testConfig{
		RouterID: "10.0.0.1",
		Routes:   []string{"10.0.0.128/32"},
		Peers:    []testPeer{{Name: "p1", Addr: "10.0.0.252", ASN: 65000}},
		Services: []testService{{Name: "web", Checks: []testCheck{{Tag: "web"}}}},
	}
}

func countEvents(events []string, op string) int {
	n := 0
	for _, e := range events {
		if e == op || strings.HasPrefix(e, op+":") {
			n++
		}
	}
	return n
}
