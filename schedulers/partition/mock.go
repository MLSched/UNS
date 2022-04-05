package partition

import "UNS/mock"

func MockPartition() *Context {
	config := mock.DLTSimulatorConfiguration()
	rmConfig := config.GetRmConfiguration()
	pc, err := Build(rmConfig.GetCluster().GetPartitions()[0])
	if err != nil {
		panic(err)
	}
	return pc
}
