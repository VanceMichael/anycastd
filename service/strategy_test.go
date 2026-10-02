package service

import (
	"encoding/json"
	"testing"

	"github.com/runityru/anycastd/checkers"
	"github.com/stretchr/testify/assert"
)

func TestNamedInstancesInStrategy(t *testing.T) {
	namedResult := func(name, group string, success bool) CheckResult {
		return CheckResult{
			checker: Checker{Name: name, Group: group},
			result:  success,
		}
	}

	// The identity of every same-kind instance must be visible to the
	// strategy input.
	var seen []string
	strategy := Strategy(func(results []CheckResult) (bool, error) {
		for _, r := range results {
			seen = append(seen, r.checker.Name)
		}
		return false, nil
	})

	_, err := strategy([]CheckResult{
		namedResult("ping-a", "g1", true),
		namedResult("ping-b", "g1", false),
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"ping-a", "ping-b"}, seen)

	// Group combination semantics key off Group only: same kind, same
	// group, one healthy instance means the group is not down.
	groupStrategy, err := GetStrategyNoOptions("all_in_group")
	assert.NoError(t, err)

	down, err := groupStrategy([]CheckResult{
		namedResult("ping-a", "g1", false),
		namedResult("ping-b", "g1", true),
	})
	assert.NoError(t, err)
	assert.False(t, down, "group is still served by the healthy sibling")

	// Both instances of the same kind/group failing brings the group down.
	down, err = groupStrategy([]CheckResult{
		namedResult("ping-a", "g1", false),
		namedResult("ping-b", "g1", false),
	})
	assert.NoError(t, err)
	assert.True(t, down)
}

func TestGetStrategy(t *testing.T) {
	_, err := GetStrategyNoOptions("all")
	assert.NoError(t, err, "all strategy exists")

	_, err = GetStrategyNoOptions("")
	assert.NoError(t, err, "should work with an empty strategy")

	_, err = GetStrategyNoOptions("unknown")
	assert.Error(t, err, "unknown strategy")

	_, err = GetStrategy("at_least_n_percentage", json.RawMessage("{\"n\": \"str\"}"))
	assert.Error(t, err, "incorrect json")

	_, err = GetStrategy("at_least_n_percentage", json.RawMessage("{\"n\": 0.5}"))
	assert.NoError(t, err)
}

func TestStrategies(t *testing.T) {
	checkM := checkers.NewMock()

	checkM.On("Kind").Return("test_check").Once()
	checkM.On("Check").Return(nil).Once()

	tests := []struct {
		strategy string
		results  []bool
		groups   []string
		expected bool
	}{
		{"all", []bool{true}, []string{""}, false},
		{"all", []bool{true, false}, []string{"", ""}, false},
		{"all", []bool{false, false}, []string{"", ""}, true},
		{"at_least_one", []bool{true, true}, []string{"", ""}, false},
		{"at_least_one", []bool{true, false}, []string{"", ""}, true},
		{"", []bool{true, false}, []string{"", ""}, true},
		{"all_in_group", []bool{true, true}, []string{"group1", "group2"}, false},
		{"all_in_group", []bool{true, false, true}, []string{"group1", "group2", "group2"}, false},
		{"all_in_group", []bool{true, false, false}, []string{"group1", "group2", "group2"}, true},
		{"all_in_group", []bool{false, true, true}, []string{"group1", "group2", "group2"}, true},
	}

	for _, testCase := range tests {
		strategy, _ := GetStrategyNoOptions(testCase.strategy)

		t.Run(testCase.strategy, func(t *testing.T) {
			checkResults := []CheckResult{}
			for i, result := range testCase.results {
				checkResults = append(checkResults, CheckResult{Checker{
					Name:  "test_check",
					Check: checkM,
					Group: testCase.groups[i],
				}, result})
			}

			serviceDown, err := strategy(checkResults)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, serviceDown)
		})
	}

	testsParams := []struct {
		strategy string
		results  []bool
		params   string
		expected bool
	}{
		{"at_least_n_percentage", []bool{true, true, true, true}, "{\"n\": 0.5}", false},
		{"at_least_n_percentage", []bool{false, true, true, true}, "{\"n\": 0.5}", false},
		{"at_least_n_percentage", []bool{false, false, true, true}, "{\"n\": 0.5}", false},
		{"at_least_n_percentage", []bool{false, false, false, true}, "{\"n\": 0.5}", true},
	}

	for _, testCase := range testsParams {
		strategy, _ := GetStrategy(testCase.strategy, json.RawMessage(testCase.params))

		t.Run(testCase.strategy, func(t *testing.T) {
			checkResults := []CheckResult{}
			for _, result := range testCase.results {
				checkResults = append(checkResults, CheckResult{Checker{
					Name:  "test_check",
					Check: checkM,
				}, result})
			}

			serviceDown, err := strategy(checkResults)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expected, serviceDown)
		})
	}
}
