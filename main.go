package main

import (
	"UNS/schedulers"
	"time"
)

func main() {
	schedulers.InitLocalSchedulersService()
	serviceInst := schedulers.GetServiceInstance()
	serviceInst.StartService()
	time.Sleep(5 * time.Second)
}
