package simulator

import (
	"testing"
)

var asyncConfigPath = "/Users/purchaser/go/src/github.com/MLSched/UNS/cases/async_simulator_configuration.json"

func TestAsyncSimulator(t *testing.T) {
	simulator := NewContinuousAsyncDLTSimulator(asyncConfigPath)
	simulator.StartSimulation()
}
