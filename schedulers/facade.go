package schedulers

import (
	"UNS/schedulers/interfaces"
	"UNS/schedulers/service"
)

func InitLocalSchedulersService() {
	service.InitSchedulersService(service.NewLocalSchedulerService())
}

func GetServiceInstance() interfaces.Service {
	return service.GetSchedulersServiceInstance()
}
