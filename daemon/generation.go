package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/pkg/errors"

	"github.com/runityru/anycastd/checkers"
	"github.com/runityru/anycastd/config"
	"github.com/runityru/anycastd/service"
)

// preparedService is a service whose checkers and strategy have all
// been constructed successfully. Until this point is reached a
// candidate configuration may still be rejected without touching the
// running generation.
type preparedService struct {
	cfg      config.Service
	checks   []service.Checker
	strategy service.Strategy

	// fp identifies the runtime behavior of the service; unchanged fp
	// means the service goroutine, its checks and its timers survive the
	// generation switch untouched.
	fp string
}

// candidate is a loaded, normalized and fully validated configuration
// that has not been applied yet. Nothing in prepare mutates runtime
// state.
type candidate struct {
	cfg *config.Config

	// fingerprint of the whole normalized configuration.
	fingerprint string

	// globalFP covers only router-id / local ASN / local address: its
	// change requires a new BGP instance.
	globalFP string

	peers    map[string]config.Peer
	prefixes []string
	services map[string]preparedService
}

// prepare loads is the first, failure-isolated stage of a generation
// switch: normalize, construct every checker and strategy, compute
// stable identities and fingerprints. Parse, validation, checker or
// strategy construction errors are returned here, before any runtime
// resource is touched.
func prepare(cfg *config.Config) (*candidate, error) {
	normalize(cfg)

	peers := make(map[string]config.Peer, len(cfg.Announcer.Peers))
	for _, p := range cfg.Announcer.Peers {
		peers[p.Name] = p
	}

	services := make(map[string]preparedService, len(cfg.Services))
	for _, svcCfg := range cfg.Services {
		checksList := make([]service.Checker, 0, len(svcCfg.Checks))
		for _, check := range svcCfg.Checks {
			c, err := checkers.NewCheckerByKind(check.Kind, check.Spec)
			if err != nil {
				return nil, errors.Wrapf(err, "error constructing checker `%s` for service `%s`", check.Kind, svcCfg.Name)
			}
			checksList = append(checksList, service.Checker{Check: c, Group: check.Group})
		}

		strategy, err := service.GetStrategy(svcCfg.Strategy, svcCfg.StrategyOptions)
		if err != nil {
			return nil, errors.Wrapf(err, "error constructing strategy for service `%s`", svcCfg.Name)
		}

		svcFP, err := serviceFingerprint(svcCfg)
		if err != nil {
			return nil, errors.Wrapf(err, "error computing service fingerprint for `%s`", svcCfg.Name)
		}

		services[svcCfg.Name] = preparedService{
			cfg:      svcCfg,
			checks:   checksList,
			strategy: strategy,
			fp:       svcFP,
		}
	}

	fp, err := configFingerprint(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "error computing configuration fingerprint")
	}

	globalFP, err := structFingerprint(cfg.Announcer.RouterID, cfg.Announcer.LocalASN, cfg.Announcer.LocalAddress)
	if err != nil {
		return nil, err
	}

	return &candidate{
		cfg:         cfg,
		fingerprint: fp,
		globalFP:    globalFP,
		peers:       peers,
		prefixes:    append([]string(nil), cfg.Announcer.Routes...),
		services:    services,
	}, nil
}

// normalize puts configuration into a canonical representation so that
// the same logical configuration always yields the same fingerprint
// regardless of map/slice ordering or JSON key ordering in check specs.
func normalize(cfg *config.Config) {
	prefixes := append([]string(nil), cfg.Announcer.Routes...)
	sort.Strings(prefixes)
	cfg.Announcer.Routes = uniqueSorted(prefixes)

	sort.Slice(cfg.Announcer.Peers, func(i, j int) bool {
		return cfg.Announcer.Peers[i].Name < cfg.Announcer.Peers[j].Name
	})

	sort.Slice(cfg.Services, func(i, j int) bool {
		return cfg.Services[i].Name < cfg.Services[j].Name
	})

	for i := range cfg.Services {
		for j := range cfg.Services[i].Checks {
			cfg.Services[i].Checks[j].Spec = canonicalJSON(cfg.Services[i].Checks[j].Spec)
		}
		cfg.Services[i].StrategyOptions = canonicalJSON(cfg.Services[i].StrategyOptions)
	}
}

func uniqueSorted(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

// canonicalJSON re-encodes a raw JSON document with sorted object keys
// and compact whitespace. Invalid or empty documents are returned
// untouched so that downstream validation reports the original error.
func canonicalJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	out, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return out
}

func configFingerprint(cfg *config.Config) (string, error) {
	// Round-trip through `any` so that maps (including the embedded check
	// specs and strategy options) are re-marshaled with sorted keys.
	data, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	var tree any
	if err := json.Unmarshal(data, &tree); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(tree)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func structFingerprint(parts ...any) (string, error) {
	canonical, err := json.Marshal(parts)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// serviceFingerprint is the stable identity of one service's runtime
// behavior. It intentionally excludes the service name, which is the
// map key used to correlate old and new generations.
func serviceFingerprint(svc config.Service) (string, error) {
	checks := make([]map[string]string, 0, len(svc.Checks))
	for _, c := range svc.Checks {
		checks = append(checks, map[string]string{
			"kind":  c.Kind,
			"group": c.Group,
			"spec":  string(canonicalJSON(c.Spec)),
		})
	}

	payload, err := json.Marshal(struct {
		Interval        string            `json:"interval"`
		Strategy        string            `json:"strategy"`
		StrategyOptions string            `json:"strategy_options"`
		Checks          []map[string]string `json:"checks"`
	}{
		Interval:        svc.CheckInterval.TimeDuration().String(),
		Strategy:        svc.Strategy,
		StrategyOptions: string(canonicalJSON(svc.StrategyOptions)),
		Checks:          checks,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
