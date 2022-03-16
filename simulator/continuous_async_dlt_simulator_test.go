package simulator

import (
	"testing"
)

var asyncConfigPath = "/Users/purchaser/go/src/UNS/cases/async_simulator_configuration.json"

func TestAsyncSimulator(t *testing.T) {
	simulator := NewContinuousAsyncDLTSimulator(asyncConfigPath)
	simulator.StartSimulation()
}
