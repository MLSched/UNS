package schedulers

import (
	"github.com/MLSched/UNS/schedulers/interfaces"
	"github.com/MLSched/UNS/schedulers/service"
)

func InitLocalSchedulersService() {
	service.InitSchedulersService(service.NewLocalSchedulerService())
}

func GetServiceInstance() interfaces.Service {
	return service.GetSchedulersServiceInstance()
}
