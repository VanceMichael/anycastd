package checkers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestCheckersRegistry(t *testing.T) {
	r := require.New(t)

	err := Register("test", newTestChecker)
	r.NoError(err)

	err = Register("test", nil)
	r.Error(err)
	r.Equal("checker with kind `test` already registered", err.Error())

	_, err = NewCheckerByKind("unknown", "unknown-instance", json.RawMessage(``))
	r.Error(err)
	r.Equal("checker with kind `unknown` is not registered", err.Error())

	c, err := NewCheckerByKind("test", "test-instance", json.RawMessage(``))
	r.NoError(err)
	r.Equal("test-instance", c.(*testChecker).name)

	err = c.Check(context.TODO())
	r.Error(err)
	r.Equal("test error", err.Error())
}

type testChecker struct {
	name string
}

func newTestChecker(name string, in json.RawMessage) (Checker, error) {
	return &testChecker{name: name}, nil
}

func (c *testChecker) Kind() string {
	return "test"
}

func (c *testChecker) Check(ctx context.Context) error {
	return errors.Errorf("test error")
}
