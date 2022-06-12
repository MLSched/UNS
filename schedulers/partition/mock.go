package partition

import "github.com/MLSched/UNS/mock"

func MockPartition() *Context {
	config := mock.DLTSimulatorConfiguration()
	rmConfig := config.GetRmConfiguration()
	pc, err := Build(rmConfig.GetCluster().GetPartitions()[0])
	if err != nil {
		panic(err)
	}
	return pc
}

func MockPartitionWithScheduler(schedulerName string) *Context {
	config := mock.DLTSimulatorConfigurationWithScheduler(schedulerName)
	rmConfig := config.GetRmConfiguration()
	pc, err := Build(rmConfig.GetCluster().GetPartitions()[0])
	if err != nil {
		panic(err)
	}
	return pc
}
