package main

import (
	"UNS/schedulers/service"
	"time"
)

func main() {
	service.InitSchedulersService()
	serviceInst := service.GetSchedulersServiceInstance()
	serviceInst.StartService()
	time.Sleep(5 * time.Second)
}
