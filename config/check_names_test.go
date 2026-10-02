package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNamedChecksFromYAML verifies the YAML -> JSON assembly path keeps
// explicit check names and that a valid same-kind multi-instance service
// passes validation unchanged.
func TestNamedChecksFromYAML(t *testing.T) {
	r := require.New(t)

	cfg, err := NewFromFile("testdata/duplicate_kind_named.yaml")
	r.NoError(err)

	checks := cfg.Services[0].Checks
	r.Len(checks, 2)

	r.Equal("uplink-a", checks[0].Name)
	r.Equal("icmp_ping", checks[0].Kind)
	r.Equal("uplinks", checks[0].Group)
	r.Equal("uplink-a", checks[0].Identity())

	r.Equal("uplink-b", checks[1].Name)
	r.Equal("icmp_ping", checks[1].Kind)
	r.Equal("uplink-b", checks[1].Identity())
}

// TestUnnamedDuplicateKindFromYAMLIsRejected proves NewFromFile fails on
// same-kind instances without names; in main() this happens while parsing
// the configuration file, before any checker, BGP peer or metrics listener
// is started.
func TestUnnamedDuplicateKindFromYAMLIsRejected(t *testing.T) {
	r := require.New(t)

	_, err := NewFromFile("testdata/duplicate_kind_unnamed.yaml")
	r.Error(err)
	r.Contains(err.Error(), `service "edge"`)
	r.Contains(err.Error(), `check #0 (kind "icmp_ping")`)
	r.Contains(err.Error(), `check #1 (kind "icmp_ping")`)
	r.Contains(err.Error(), "name is required")
}
