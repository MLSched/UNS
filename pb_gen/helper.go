package pb_gen

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
	"encoding/json"
)

var extractSchedulerConfigurationMap = map[configs.SchedulerType]func(bytes []byte) (interface{}, error){
	configs.SchedulerType_schedulerTypeNaive: func(bytes []byte) (interface{}, error) {
		c := &configs.NaiveSchedulerConfiguration{}
		err := json.Unmarshal(bytes, c)
		return c, err
	},
	configs.SchedulerType_schedulerTypeUNS: func(bytes []byte) (interface{}, error) {
		c := &configs.UNSSchedulerConfiguration{}
		err := json.Unmarshal(bytes, c)
		return c, err
	},
}

func GetNaiveSchedulerConfiguration(configuration *configs.SchedulerConfiguration) *configs.NaiveSchedulerConfiguration {
	return ExtractSchedulerConfiguration(configuration).(*configs.NaiveSchedulerConfiguration)
}

func GetUNSSchedulerConfiguration(configuration *configs.SchedulerConfiguration) *configs.UNSSchedulerConfiguration {
	return ExtractSchedulerConfiguration(configuration).(*configs.UNSSchedulerConfiguration)
}

func ExtractSchedulerConfiguration(configuration *configs.SchedulerConfiguration) interface{} {
	c, err := extractSchedulerConfigurationMap[configuration.GetSchedulerType()](configuration.GetConfigurationBytes())
	if err != nil {
		panic(err)
	}
	return c
}

var extractPredictorConfigurationMap = map[configs.PredictorType]func(bytes []byte) (interface{}, error){
	configs.PredictorType_predictorTypeDLTRandom: func(bytes []byte) (interface{}, error) {
		c := &configs.DLTPredictorRandomConfiguration{}
		err := json.Unmarshal(bytes, c)
		return c, err
	},
	configs.PredictorType_predictorTypeDLTDataOriented: func(bytes []byte) (interface{}, error) {
		c := &configs.DLTPredictorDataOrientedConfiguration{}
		err := json.Unmarshal(bytes, c)
		return c, err
	},
}

func GetRandomPredictorConfiguration(configuration *configs.PredictorConfiguration) *configs.DLTPredictorRandomConfiguration {
	return ExtractPredictorConfiguration(configuration).(*configs.DLTPredictorRandomConfiguration)
}

func GetDataOrientedPredictorConfiguration(configuration *configs.PredictorConfiguration) *configs.DLTPredictorDataOrientedConfiguration {
	return ExtractPredictorConfiguration(configuration).(*configs.DLTPredictorDataOrientedConfiguration)
}

func ExtractPredictorConfiguration(configuration *configs.PredictorConfiguration) interface{} {
	c, err := extractPredictorConfigurationMap[configuration.GetPredictorType()](configuration.GetConfigurationBytes())
	if err != nil {
		panic(err)
	}
	return c
}

func GetAllocatedAcceleratorIDs(allocation *objects.JobAllocation) []string {
	acceleratorIDs := make([]string, 0)
	for _, taskAllocation := range allocation.GetTaskAllocations() {
		acceleratorIDs = append(acceleratorIDs, taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
	}
	return acceleratorIDs
}
