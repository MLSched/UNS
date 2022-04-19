package mock

import (
	"github.com/MLSched/UNS/pb_gen/configs"
	"github.com/MLSched/UNS/utils"
	"io/ioutil"
)

var simulatorConfigurationPath = "/Users/purchaser/go/src/github.com/MLSched/UNS/cases/sync_simulator_configuration.json"

func DLTSimulatorConfiguration() *configs.DLTSimulatorConfiguration {
	config := &configs.DLTSimulatorConfiguration{}
	bytes, err := ioutil.ReadFile(simulatorConfigurationPath)
	if err != nil {
		panic(err)
	}
	err = utils.Unmarshal(string(bytes), config)
	if err != nil {
		panic(err)
	}
	return config
}
