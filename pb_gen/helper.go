package pb_gen

import (
	"UNS/pb_gen/configs"
	"UNS/pb_gen/objects"
)

var extractSchedulerConfigurationMap = map[configs.SchedulerType]func(configuration *configs.SchedulerConfiguration) interface{}{
	configs.SchedulerType_schedulerTypeNaive: func(configuration *configs.SchedulerConfiguration) interface{} {
		return configuration.GetNaiveSchedulerConfiguration()
	},
	configs.SchedulerType_schedulerTypeUNS: func(configuration *configs.SchedulerConfiguration) interface{} {
		return configuration.GetUnsSchedulerConfiguration()
	},
}

func GetNaiveSchedulerConfiguration(configuration *configs.SchedulerConfiguration) *configs.NaiveSchedulerConfiguration {
	return ExtractSchedulerConfiguration(configuration).(*configs.NaiveSchedulerConfiguration)
}

func GetUNSSchedulerConfiguration(configuration *configs.SchedulerConfiguration) *configs.UNSSchedulerConfiguration {
	return ExtractSchedulerConfiguration(configuration).(*configs.UNSSchedulerConfiguration)
}

func ExtractSchedulerConfiguration(configuration *configs.SchedulerConfiguration) interface{} {
	c := extractSchedulerConfigurationMap[configuration.GetSchedulerType()](configuration)
	return c
}

var extractPredictorConfigurationMap = map[configs.PredictorType]func(configuration *configs.PredictorConfiguration) interface{}{
	configs.PredictorType_predictorTypeDLTRandom: func(configuration *configs.PredictorConfiguration) interface{} {
		return configuration.GetDLTPredictorRandomConfiguration()
	},
	configs.PredictorType_predictorTypeDLTDataOriented: func(configuration *configs.PredictorConfiguration) interface{} {
		return configuration.GetDLTPredictorDataOrientedDataFormat()
	},
}

func GetRandomPredictorConfiguration(configuration *configs.PredictorConfiguration) *configs.DLTPredictorRandomConfiguration {
	return ExtractPredictorConfiguration(configuration).(*configs.DLTPredictorRandomConfiguration)
}

func GetDataOrientedPredictorConfiguration(configuration *configs.PredictorConfiguration) *configs.DLTPredictorDataOrientedConfiguration {
	return ExtractPredictorConfiguration(configuration).(*configs.DLTPredictorDataOrientedConfiguration)
}

func ExtractPredictorConfiguration(configuration *configs.PredictorConfiguration) interface{} {
	c := extractPredictorConfigurationMap[configuration.GetPredictorType()](configuration)
	return c
}

func GetAllocatedAcceleratorIDs(allocation *objects.JobAllocation) []string {
	acceleratorIDs := make([]string, 0)
	for _, taskAllocation := range allocation.GetTaskAllocations() {
		acceleratorIDs = append(acceleratorIDs, taskAllocation.GetAcceleratorAllocation().GetAcceleratorID())
	}
	return acceleratorIDs
}
