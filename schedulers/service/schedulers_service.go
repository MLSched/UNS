package service

import (
	"UNS/events"
	eventsobjs "UNS/pb_gen/events"
	"UNS/resourcemgr"
	"UNS/schedulers/cluster"
	"UNS/schedulers/impls"
	"UNS/schedulers/interfaces"
	"UNS/schedulers/partition"
	"fmt"
	"sync"
)

var instance interfaces.Service

func InitSchedulersService() {
	cluster.InitClusterManager()
	instance = &SchedulersService{
		mu:                        &sync.RWMutex{},
		ResourceManagerID2Mapping: make(map[string][]*Mapping),
		SchedulerID2Mapping:       make(map[string]*Mapping),
		PendingEvents:             make(chan eventWithSource, 1024*1024),
	}
}

func GetSchedulersServiceInstance() interfaces.Service {
	return instance
}

// SchedulersService 统一调度服务的大脑。
// 管理ResourceManager与Cluster、Partition、Scheduler的引用以及维护它们的映射关系。
// 并且负责分发两侧之间的消息。
type SchedulersService struct {
	mu                        *sync.RWMutex
	ResourceManagerID2Mapping map[string][]*Mapping
	SchedulerID2Mapping       map[string]*Mapping

	PendingEvents chan eventWithSource
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

type Mapping struct {
	ResourceManager  resourcemgr.Interface
	ClusterContext   *cluster.Context
	PartitionContext *partition.Context
	Scheduler        interfaces.Scheduler
}

func (ss *SchedulersService) StartService() {
	go func() {
		for {
			select {
			case e := <-ss.PendingEvents:
				{
					go func() {
						switch e := e.(type) {
						case *eventFromRM:
							ss.handleEventFromRM(e)
						case *eventFromScheduler:
							ss.handleEventFromScheduler(e)
						}
					}()
				}
			}
		}
	}()
}

func (ss *SchedulersService) PushFromRM(rmID string, partitionID string, event *events.Event) {
	ss.PendingEvents <- &eventFromRM{
		Event:       event,
		RMID:        rmID,
		PartitionID: partitionID,
	}
}

func (ss *SchedulersService) RegisterRM(event *eventsobjs.RMRegisterResourceManagerEvent, resourceMgr resourcemgr.Interface) *events.Result {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	clusterContext, err := cluster.Build(event.Configuration.Cluster)
	if err != nil {
		return &events.Result{
			Succeeded: false,
			Reason:    fmt.Sprintf("RegisterRM failed, cluster Build failed, err = [%s]", err),
		}
	}
	partitionContexts := clusterContext.GetPartitionContexts()
	partitionID2schedulerConfigurations := event.GetConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()
	partitionID2Scheduler := make(map[string]interfaces.Scheduler)
	for partitionID, c := range partitionID2schedulerConfigurations {
		s, err := impls.Build(c, ss.PushFromScheduler)
		if err != nil {
			return &events.Result{
				Succeeded: false,
				Reason:    fmt.Sprintf("RegisterRM failed, scheduler Build failed, err = [%s]", err),
			}
		}
		partitionID2Scheduler[partitionID] = s
	}
	mappings := make([]*Mapping, 0, len(partitionContexts))
	for _, partitionContext := range partitionContexts {
		mapping := &Mapping{
			ResourceManager:  resourceMgr,
			ClusterContext:   clusterContext,
			PartitionContext: partitionContext,
			Scheduler:        partitionID2Scheduler[partitionContext.Meta.GetPartitionID()],
		}
		mappings = append(mappings, mapping)
		mapping.Scheduler.StartService()
	}
	return &events.Result{
		Succeeded: true,
	}
}

func (ss *SchedulersService) PushFromScheduler(schedulerID string, event *events.Event) {
	ss.PendingEvents <- &eventFromScheduler{
		Event:       event,
		SchedulerID: schedulerID,
	}
}

func (ss *SchedulersService) handleEventFromRM(e *eventFromRM) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	mappings, ok := ss.ResourceManagerID2Mapping[e.RMID]
	if !ok {
		events.Reply(e.Event.ResultChan, &events.Result{
			Succeeded: false,
			Reason:    fmt.Sprintf("Non-registered RMID, RMID = [%s]", e.RMID),
		})
	}
	for _, mapping := range mappings {
		if mapping.PartitionContext.Meta.GetPartitionID() == e.PartitionID {
			mapping.PartitionContext.HandleEvent(e.Event)
			events.ReplySucceeded(e.Event.ResultChan)
			return
		}
	}
	events.Reply(e.Event.ResultChan, &events.Result{
		Succeeded: false,
		Reason:    fmt.Sprintf("Non-existed Partition ID, RMID = [%s], Partition ID = [%s]", e.RMID, e.PartitionID),
	})
}

func (ss *SchedulersService) handleEventFromScheduler(e *eventFromScheduler) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	mapping, ok := ss.SchedulerID2Mapping[e.SchedulerID]
	if !ok {
		events.Reply(e.Event.ResultChan, &events.Result{
			Succeeded: false,
			Reason:    fmt.Sprintf("Non-existed Scheduler ID, Scheduler ID = [%s]", e.SchedulerID),
		})
	}
	mapping.ResourceManager.HandleEvent(e.Event)
	events.ReplySucceeded(e.Event.ResultChan)
}
