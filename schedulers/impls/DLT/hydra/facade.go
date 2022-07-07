package hydra

import (
	"fmt"
	"github.com/MLSched/UNS/pb_gen"
	"github.com/MLSched/UNS/pb_gen/configs"
	eventsobjs "github.com/MLSched/UNS/pb_gen/events"
	"github.com/MLSched/UNS/pb_gen/objects"
	"github.com/MLSched/UNS/predictor"
	interfaces2 "github.com/MLSched/UNS/predictor/interfaces"
	base2 "github.com/MLSched/UNS/schedulers/impls/DLT/base"
	"github.com/MLSched/UNS/schedulers/impls/DLT/hydra/adapter"
	"github.com/MLSched/UNS/schedulers/impls/DLT/hydra/hydra_scheduler"
	"github.com/MLSched/UNS/schedulers/impls/DLT/hydra/hydra_scheduler/cost"
	"github.com/MLSched/UNS/schedulers/impls/DLT/hydra/types"
	"github.com/MLSched/UNS/schedulers/interfaces"
	"github.com/golang/protobuf/ptypes/wrappers"
	"time"
)

type Scheduler struct {
	*base2.DLTSchedulerTemplate

	Config    *configs.HydraSchedulerConfiguration
	Predictor interfaces2.Predictor
}

func (s *Scheduler) GetSchedulerID() string {
	return s.Config.GetSchedulerID()
}

func Build(configuration interface{}, pusher base2.EventPusher, partitionContextAware base2.PartitionContextAware) (interfaces.Scheduler, error) {
	c := configuration.(*configs.HydraSchedulerConfiguration)
	sche := &Scheduler{
		Config:    c,
		Predictor: predictor.BuildPredictor(c.PredictorConfiguration),
	}
	sche.DLTSchedulerTemplate = base2.NewIntervalSchedulerTemplate(sche, c.GetIntervalNano(), partitionContextAware, c.GetSyncMode(), pusher)
	sche.DLTSchedulerTemplate.SetSupportTaskGroupTypes([]objects.TaskGroupType{objects.TaskGroupType_taskGroupTypeSingle})
	return sche, nil
}

func (s *Scheduler) DoSchedule() *eventsobjs.SSUpdateAllocationsEvent {
	pc := s.GetPartitionContext().Clone(false)
	t := pc.Now()
	pc.Time = &t
	if len(pc.AllocationViews.UnallocatedJobs) == 0 {
		return nil
	}

	ctx := adapter.BuildScheduleContext(pc, s.Predictor)
	//scheduler := initHydraBABHeuristicScheduler(10000 * time.Millisecond)
	//initHydraBABHeuristicScheduler(100 * time.Millisecond)
	scheduler := initHydraHeuristicScheduler()
	//scheduler := initHydraBABHeuristicScheduler(1000 * time.Millisecond)
	//scheduler := initHydraBABHeuristicScheduler(5 * time.Millisecond)
	scheduler.SetCluster(ctx.Cluster)
	scheduler.OnScheduleEvent(types.NewScheduleEventJobsArrived(ctx.UnallocatedJobMetas))
	unallocatedJobs := ctx.PC.AllocationViews.UnallocatedJobs
	newJobAllocations := make([]*pb_gen.JobAllocation, 0)
	for _, queue := range ctx.Cluster.GPUJobQueues() {
		if len(queue.Jobs()) == 0 {
			continue
		}
		start := pc.FixedNow()
		for _, job := range queue.Jobs() {
			j := job.(*adapter.Job).Job
			if _, ok := unallocatedJobs[j.GetJobID()]; !ok {
				panic(fmt.Sprintf("unallocated job ? %v", j))
			}
			task := j.GetTaskGroup().GetTasks()[0]
			accID := queue.GPU().AccID()
			taskAllocation := &objects.TaskAllocation{
				NodeID:                   ctx.PC.MetalViews.AcceleratorID2NodeID[accID],
				JobID:                    j.GetJobID(),
				TaskID:                   task.GetTaskID(),
				AllocationTimeNanoSecond: ctx.PC.FixedNow(),
				AcceleratorAllocation: &objects.AcceleratorAllocation{
					AcceleratorID: accID,
				},
			}
			jobAllocation := &pb_gen.JobAllocation{
				JobAllocation: &objects.JobAllocation{
					JobID:             j.GetJobID(),
					ResourceManagerID: ctx.PC.Meta.GetResourceManagerID(),
					PartitionID:       ctx.PC.Meta.GetPartitionID(),
					TaskAllocations:   []*objects.TaskAllocation{taskAllocation},
					Extra:             nil,
				},
			}
			taskAllocation.StartExecutionTimeNanoSecond = &wrappers.Int64Value{Value: start}
			pr, err := s.Predictor.PredictSolely(ctx.PC, []*pb_gen.JobAllocation{jobAllocation})
			if err != nil {
				panic(err)
			}
			r := pr.GetResult(taskAllocation)
			start = *r.GetFinishNanoTime()
			newJobAllocations = append(newJobAllocations, jobAllocation)
		}
	}
	return &eventsobjs.SSUpdateAllocationsEvent{NewJobAllocations: pb_gen.UnwrapJobAllocations(newJobAllocations)}
}

func initHydraBABHeuristicScheduler(latency time.Duration) types.Scheduler {
	return hydra_scheduler.New(
		hydra_scheduler.WithScheme(hydra_scheduler.NewBasicScheduleScheme(true, false, -1, true)),
		hydra_scheduler.WithDistanceAlgo(hydra_scheduler.NewMinCostDistanceAlgo(
			//cost.NewBranchAndBoundAlgoWithLatency(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL, time.Duration(latencySec)*time.Second, cost.NewSwapHeuristic()),
			cost.NewBranchAndBoundAlgoWithLatency(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL, latency, cost.NewSwapHeuristic()),
			cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e9))),
	)
}

func initHydraHeuristicScheduler() types.Scheduler {
	return hydra_scheduler.New(
		hydra_scheduler.WithScheme(hydra_scheduler.NewBasicScheduleScheme(true, false, -1, true)),
		hydra_scheduler.WithDistanceAlgo(hydra_scheduler.NewMinCostDistanceAlgo(
			//cost.NewBranchAndBoundAlgoWithLatency(cost.BranchAndBoundLCStandardPredictCost, cost.BranchAndBoundAlgoTypeFixNonDDL, time.Duration(latencySec)*time.Second, cost.NewSwapHeuristic()),
			cost.NewSwapHeuristic(),
			cost.NewSimpleAddCostSolverMaker(cost.DDLCostTypeStrict, 1e9))),
	)
}

func (s *Scheduler) GetPredictor() interfaces2.Predictor {
	return s.Predictor
}
