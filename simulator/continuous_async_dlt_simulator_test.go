package simulator

import (
	"testing"
)

var asyncConfigPath = "/Users/purchaser/go/src/UNS/cases/async_simulator_configuration.json"

func init() {
	//f, err := os.OpenFile("/Users/purchaser/go/src/UNS/logs/simulator_test.log", os.O_CREATE, 0666)
	//if err != nil {
	//	panic(err)
	//}
	//log.SetOutput(f)
}

func TestAsyncSimulator(t *testing.T) {
	simulator := NewContinuousAsyncDLTSimulator(asyncConfigPath)
	simulator.StartSimulation()
}
