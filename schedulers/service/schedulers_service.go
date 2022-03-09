package service

import (
	"UNS/events"
	"UNS/schedulers/interfaces"
)

var instance interfaces.Service

func InitSchedulersService(service interfaces.Service) {
	instance = service
}

func GetSchedulersServiceInstance() interfaces.Service {
	return instance
}

type eventWithSource interface{}

type eventFromRM struct {
	*events.Event
	RMID        string
	PartitionID string
}

type eventFromScheduler struct {
	*events.Event
	SchedulerID string
}
