package service

import (
	"fmt"
	"github.com/MLSched/UNS/events"
	eventsobjs "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/resourcemgr"
	"github.com/MLSched/UNS/schedulers/cluster"
	"github.com/MLSched/UNS/schedulers/impls"
	"github.com/MLSched/UNS/schedulers/impls/DLT/base"
	"github.com/MLSched/UNS/schedulers/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
	"sync"
)

// LocalSchedulersService 统一调度服务的大脑。
// 管理ResourceManager与Cluster、MockPartition、Scheduler的引用以及维护它们的映射关系。
// 并且负责分发两侧之间的消息。
type LocalSchedulersService struct {
	mu                        *sync.RWMutex
	clusterManager            *cluster.Manager
	ResourceManagerID2Mapping map[string][]*Mapping
	SchedulerID2Mapping       map[string]*Mapping

	PendingEvents chan eventWithSource
}

type Mapping struct {
	ResourceManager  resourcemgr.Interface
	ClusterContext   *cluster.Context
	PartitionContext *partition.Context
	Scheduler        interfaces.Scheduler
}

func NewLocalSchedulerService() *LocalSchedulersService {
	return &LocalSchedulersService{
		mu:                        &sync.RWMutex{},
		clusterManager:            cluster.NewManager(),
		ResourceManagerID2Mapping: make(map[string][]*Mapping),
		SchedulerID2Mapping:       make(map[string]*Mapping),
		PendingEvents:             make(chan eventWithSource, 1024*1024),
	}
}

func (ss *LocalSchedulersService) StartService() {
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

func (ss *LocalSchedulersService) Push(rmID string, partitionID string, event *events.Event) {
	ss.PendingEvents <- &eventFromRM{
		Event:       event,
		RMID:        rmID,
		PartitionID: partitionID,
	}
}

func (ss *LocalSchedulersService) RegisterRM(event *eventsobjs.RMRegisterResourceManagerEvent, resourceMgr resourcemgr.Interface) *events.Result {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	clusterContext, err := cluster.Build(event.Configuration.Cluster)
	if err != nil {
		return &events.Result{
			Succeeded: false,
			Reason:    fmt.Sprintf("RegisterRM failed, cluster Build failed, err = [%s]", err),
		}
	}
	clusterContext.Meta.GetResourceManagerID()
	err = ss.clusterManager.AddClusterContext(clusterContext)
	if err != nil {
		return &events.Result{
			Succeeded: false,
			Reason:    fmt.Sprintf("RegisterRM failed, add cluster context failed, err = [%s]", err),
		}
	}

	partitionContexts := clusterContext.GetPartitionContexts()
	partitionID2schedulerConfigurations := event.GetConfiguration().GetSchedulersConfiguration().GetPartitionID2SchedulerConfiguration()
	partitionID2Scheduler := make(map[string]interfaces.Scheduler)
	for partitionID, c := range partitionID2schedulerConfigurations {
		s, err := impls.Build(&base.SchedulerBuildParams{
			SchedulerConfiguration: c,
			EventPusher:            ss.pushFromScheduler,
			PartitionContextAware: func() *partition.Context {
				pc, err := ss.clusterManager.GetPartitionContext(resourceMgr.GetResourceManagerID(), partitionID)
				if err != nil {
					panic(err)
				}
				return pc
			},
		})
		if err != nil {
			return &events.Result{
				Succeeded: false,
				Reason:    fmt.Sprintf("RegisterRM failed, scheduler Build failed, err = [%s]", err),
			}
		}
		partitionID2Scheduler[partitionID] = s
	}
	// build mappings
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
	// register mappings
	ss.ResourceManagerID2Mapping[resourceMgr.GetResourceManagerID()] = mappings
	for _, mapping := range mappings {
		ss.SchedulerID2Mapping[mapping.Scheduler.GetSchedulerID()] = mapping
	}
	return &events.Result{
		Succeeded: true,
	}
}

func (ss *LocalSchedulersService) pushFromScheduler(schedulerID string, event *events.Event) {
	ss.PendingEvents <- &eventFromScheduler{
		Event:       event,
		SchedulerID: schedulerID,
	}
}

func (ss *LocalSchedulersService) handleEventFromRM(e *eventFromRM) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	mappings, ok := ss.ResourceManagerID2Mapping[e.RMID]
	if !ok {
		events.Reply(e.Event, &events.Result{
			Succeeded: false,
			Reason:    fmt.Sprintf("Non-registered RMID, RMID = [%s]", e.RMID),
		})
	}
	for _, mapping := range mappings {
		if mapping.PartitionContext.Meta.GetPartitionID() == e.PartitionID {
			go func() {
				mapping.Scheduler.HandleEvent(e.Event)
			}()
			return
		}
	}
	events.Reply(e.Event, &events.Result{
		Succeeded: false,
		Reason:    fmt.Sprintf("Non-existed MockPartition ID, RMID = [%s], MockPartition ID = [%s]", e.RMID, e.PartitionID),
	})
}

func (ss *LocalSchedulersService) handleEventFromScheduler(e *eventFromScheduler) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	mapping, ok := ss.SchedulerID2Mapping[e.SchedulerID]
	if !ok {
		events.Reply(e.Event, &events.Result{
			Succeeded: false,
			Reason:    fmt.Sprintf("Non-existed Scheduler ID, Scheduler ID = [%s]", e.SchedulerID),
		})
	}
	go func() {
		mapping.ResourceManager.HandleEvent(e.Event)
	}()
}
