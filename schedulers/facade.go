package schedulers

import (
	"UNS/schedulers/cluster"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/service"
)

func InitLocalSchedulersService() {
	cluster.InitClusterManager()
	service.InitSchedulersService(service.NewLocalSchedulerService())
}

func GetServiceInstance() interfaces.Service {
	return service.GetSchedulersServiceInstance()
}
