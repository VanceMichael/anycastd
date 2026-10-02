package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/asaskevich/govalidator"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
	"github.com/pkg/errors"
	th "github.com/teran/go-time"
	yaml "gopkg.in/yaml.v3"
)

var isCIDR = validation.NewStringRuleWithError(govalidator.IsCIDR, validation.NewError("validation_is_cidr", "must be a valid CIDR"))

// checkNamePattern restricts explicit check names so they stay safe as
// Prometheus label values and log tokens. The name is never used as a part
// of a metric name, only as its value.
var checkNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

const checkNameMaxLength = 128

var (
	_ validation.Validatable = (*Config)(nil)
	_ validation.Validatable = (*Announcer)(nil)
	_ validation.Validatable = (*Service)(nil)
	_ validation.Validatable = (*Metrics)(nil)
	_ validation.Validatable = (*Check)(nil)
	_ validation.Validatable = (*Peer)(nil)
)

type Announcer struct {
	RouterID     string   `json:"router_id"`
	LocalAddress string   `json:"local_address"`
	LocalASN     uint32   `json:"local_asn"`
	Routes       []string `json:"routes"`
	Peers        []Peer   `json:"peers"`
}

func (a Announcer) Validate() error {
	return validation.ValidateStruct(&a,
		validation.Field(&a.RouterID, validation.Required, is.IPv4),
		validation.Field(&a.LocalAddress, validation.Required, is.IPv4),
		validation.Field(&a.LocalASN, validation.Required),
		validation.Field(&a.Routes, validation.Required, validation.Each(isCIDR)),
		validation.Field(&a.Peers, validation.Required),
	)
}

type Check struct {
	// Name is the explicit, stable identity of a check instance within a
	// service. It is required whenever the same Kind appears more than once
	// in a service and must be unique within the service. An empty Name is
	// allowed only for checks whose Kind is unique; such checks keep the
	// Kind itself as their runtime identity and metric label value.
	Name  string          `json:"name"`
	Kind  string          `json:"kind"`
	Spec  json.RawMessage `json:"spec"`
	Group string          `json:"group"`
}

// Identity returns the stable runtime identifier of the check instance:
// the explicit Name when set, otherwise the Kind (legacy behavior).
func (c Check) Identity() string {
	if c.Name != "" {
		return c.Name
	}
	return c.Kind
}

func (c Check) Validate() error {
	return validation.ValidateStruct(&c,
		validation.Field(&c.Kind, validation.Required),
		validation.Field(&c.Spec, validation.Required),
		validation.Field(&c.Name, validation.When(c.Name != "",
			validation.Length(1, checkNameMaxLength),
			validation.Match(checkNamePattern),
		)),
	)
}

type Peer struct {
	Name           string `json:"name"`
	RemoteAddress  string `json:"remote_address"`
	RemoteASN      uint32 `json:"remote_asn"`
	EnableMultihop bool   `json:"enable_multihop"`
	MultihopTTL    uint32 `json:"multihop_ttl"`
}

func (p Peer) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.Name, validation.Required),
		validation.Field(&p.RemoteAddress, validation.Required, is.IPv4),
		validation.Field(&p.RemoteASN, validation.Required),
		validation.Field(&p.MultihopTTL, validation.When(p.EnableMultihop, validation.Min(uint32(1)))),
	)
}

type Service struct {
	Name            string          `json:"name"`
	CheckInterval   th.Duration     `json:"check_interval"`
	Strategy        string          `json:"strategy"`
	StrategyOptions json.RawMessage `json:"strategy_options"`
	Checks          []Check         `json:"checks"`
}

func (s Service) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.Name, validation.Required),
		validation.Field(&s.CheckInterval, validation.Required),
		validation.Field(&s.Checks, validation.Required, validation.By(func(value interface{}) error {
			return validateChecks(s.Name, value)
		})),
	)
}

// validateChecks enforces the per-service check identity rules:
//   - every instance of a Kind that appears more than once must carry an
//     explicit Name;
//   - explicit Names must be unique within the service;
//   - the resolved identities (Name, or Kind when Name is empty) must not
//     collide, so an explicit Name can never shadow another check's
//     Kind-derived identity.
//
// Errors are keyed by the check index so they locate the offending
// service/check pair during configuration parsing, before any runtime
// component is started.
func validateChecks(serviceName string, value interface{}) error {
	checks, ok := value.([]Check)
	if !ok {
		return nil
	}

	kindCount := map[string]int{}
	for _, c := range checks {
		kindCount[c.Kind]++
	}

	errs := validation.Errors{}
	names := map[string]int{}
	identities := map[string]int{}

	for i, c := range checks {
		if kindCount[c.Kind] > 1 && c.Name == "" {
			errs[fmt.Sprintf("check #%d (kind %q)", i, c.Kind)] = errors.Errorf(
				"name is required: kind %q appears %d times in service %q, each instance must have a unique name",
				c.Kind, kindCount[c.Kind], serviceName,
			)
		}

		if c.Name != "" {
			if first, ok := names[c.Name]; ok {
				errs[fmt.Sprintf("check #%d (name %q)", i, c.Name)] = errors.Errorf(
					"duplicate check name %q: already used by check #%d in service %q",
					c.Name, first, serviceName,
				)
			} else {
				names[c.Name] = i
			}
		}

		identity := c.Identity()
		if first, ok := identities[identity]; ok {
			errs[fmt.Sprintf("check #%d (identity %q)", i, identity)] = errors.Errorf(
				"check identity %q collides with check #%d in service %q; rename the check or use a unique kind",
				identity, first, serviceName,
			)
		} else {
			identities[identity] = i
		}
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

type Metrics struct {
	Enabled bool   `json:"enabled"`
	Address string `json:"address"`
}

func (m Metrics) Validate() error {
	return validation.ValidateStruct(&m,
		validation.Field(&m.Enabled, validation.Required),
		validation.Field(&m.Address, validation.Required),
	)
}

type Config struct {
	Announcer Announcer `json:"announcer"`
	Services  []Service `json:"services"`
	Metrics   Metrics   `json:"metrics"`
}

func (c *Config) Validate() error {
	return validation.ValidateStruct(c,
		validation.Field(&c.Announcer),
		validation.Field(&c.Services, validation.Required),
		validation.Field(&c.Metrics),
	)
}

func NewFromFile(filename string) (*Config, error) {
	cfg := &Config{}

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, errors.Wrap(err, "error reading configuration file")
	}

	ext := strings.ToLower(filepath.Ext(filename))

	switch ext {
	case ".yml", ".yaml":
		type intermediate struct {
			Announcer map[string]any `yaml:"announcer"`
			Services  []any          `yaml:"services"`
			Metrics   map[string]any `yaml:"metrics"`
		}

		d := intermediate{}

		err := yaml.Unmarshal(data, &d)
		if err != nil {
			return nil, errors.Wrap(err, "error unmarshaling intermediate configuration")
		}

		data, err = json.Marshal(d)
		if err != nil {
			return nil, errors.Wrap(err, "error marshaling intermediate configuration")
		}
	case ".json":
		// no additional action needed
	default:
		return nil, errors.Errorf("unexpected file format: `%s`", ext)
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, errors.Wrap(err, "error unmarshaling config")
	}

	return cfg, cfg.Validate()
}
