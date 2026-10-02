package checkers

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pkg/errors"
)

type Checker interface {
	Kind() string
	Check(ctx context.Context) error
}

// Constructor builds a Checker instance. name is the stable per-service
// identity of the check instance (explicit check name, or the kind itself
// for legacy single-instance configurations); checkers which own metrics
// or emit logs must use it instead of the kind constant so multiple
// instances of the same kind can never overwrite each other.
type Constructor func(name string, in json.RawMessage) (Checker, error)

var (
	registry      = map[string]Constructor{}
	registryMutex = &sync.RWMutex{}
)

func NewCheckerByKind(kind, name string, spec json.RawMessage) (Checker, error) {
	registryMutex.RLock()
	defer registryMutex.RUnlock()

	c, ok := registry[kind]
	if !ok {
		return nil, errors.Errorf("checker with kind `%s` is not registered", kind)
	}

	return c(name, spec)
}

func Register(kind string, fn Constructor) error {
	registryMutex.Lock()
	defer registryMutex.Unlock()

	if _, ok := registry[kind]; ok {
		return errors.Errorf("checker with kind `%s` already registered", kind)
	}

	registry[kind] = fn

	return nil
}

func MustRegister(kind string, fn Constructor) {
	if err := Register(kind, fn); err != nil {
		panic(err)
	}
}
