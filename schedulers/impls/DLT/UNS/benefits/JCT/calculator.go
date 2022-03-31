package JCT

import (
	"UNS/pb_gen/objects"
	predictorinterfaces "UNS/predictor/interfaces"
	"UNS/schedulers/impls/DLT/UNS/benefits/base"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/partition"
	"sort"
)

type Calculator struct {
	*base.CalculatorCommon
}

func NewCalculator() *Calculator {
	common := &base.CalculatorCommon{}
	c := &Calculator{}
	common.Impl = c
	c.CalculatorCommon = common
	return c
}

type Stub struct {
	JobID2JCT map[string]int64
}

//func (c *Calculator) ByPredictIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
//	return c.Cal(pc, prevStub, func() map[string]*base.BenefitCalculationContext {
//		return c.ExtractContextByPredict(pc, allocationsPredictResult)
//	})
//}

func (c *Calculator) ByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) (benefit interfaces2.Benefit, stub interface{}) {
	return 0, nil
}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{JobID2JCT: make(map[string]int64)}
	oStub := stub.(*Stub)
	for jobID, JCT := range oStub.JobID2JCT {
		s.JobID2JCT[jobID] = JCT
	}
	return s
}

//func (c *Calculator) ByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
//	return c.Cal(pc, nil, func() map[string]*base.BenefitCalculationContext {
//		return c.ExtractContextByPredict(pc, allocationsPredictResult)
//	})
//}

//func (c *Calculator) updateStubByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, stub *Stub) {
//	if allocationsPredictResult == nil {
//		return
//	}
//	allocationsPredictResult.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
//		job := pc.GetUnfinishedJob(allocation.GetJobID())
//		submitTime := job.GetSubmitTimeNanoSecond()
//		finishTime := *result.GetFinishNanoTime()
//		stub.JobID2JCT[job.GetJobID()] = finishTime - submitTime
//	})
//}

func (c *Calculator) UpdateStub(pc *partition.Context, contexts map[string]*base.BenefitCalculationContext, stub interface{}) {
	s := stub.(*Stub)
	for _, ctx := range contexts {
		job := ctx.Job
		submitTime := job.GetSubmitTimeNanoSecond()
		finishTime := ctx.FinishTime
		s.JobID2JCT[job.GetJobID()] = finishTime - submitTime
	}
}

//func (c *Calculator) updateStubByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory, stub *Stub) {
//	if histories == nil {
//		return
//	}
//	jobID2JCT := make(map[string]int64)
//	for _, history := range histories {
//		for _, taskHistory := range history.GetTaskExecutionHistories() {
//			if !taskHistory.GetFinished() {
//				continue
//			}
//			if _, ok := jobID2JCT[taskHistory.GetJobID()]; ok {
//				continue
//			}
//			job := pc.GetJob(taskHistory.GetJobID())
//			if job == nil {
//				continue
//			}
//			submitTime := job.GetSubmitTimeNanoSecond()
//			finishTime := taskHistory.GetStartExecutionTimeNanoSecond() + taskHistory.GetDurationNanoSecond()
//			jobID2JCT[job.GetJobID()] = finishTime - submitTime
//		}
//	}
//	for jobID, JCT := range jobID2JCT {
//		stub.JobID2JCT[jobID] = JCT
//	}
//}

func (c *Calculator) calculateAvgJCT(stub *Stub) float64 {
	totalJCT := int64(0)
	for _, JCT := range stub.JobID2JCT {
		totalJCT += JCT
	}
	avgJCT := float64(totalJCT) / float64(len(stub.JobID2JCT))
	return avgJCT
}

func (c *Calculator) avgJCT2Benefit(avgJCT float64) interfaces2.Benefit {
	return interfaces2.Benefit(-avgJCT)
}

func (c *Calculator) NewStub() interface{} {
	return &Stub{JobID2JCT: make(map[string]int64)}
}

func (c *Calculator) Calculate(prevStub interface{}) interfaces2.Benefit {
	avgJCT := c.calculateAvgJCT(prevStub.(*Stub))
	return c.avgJCT2Benefit(avgJCT)
}

func (c *Calculator) PrioritySort(pc *partition.Context, jobs map[string]*objects.Job, predictor predictorinterfaces.Predictor) map[string]int {
	type JobAndTime struct {
		Job  *objects.Job
		Time int64
	}
	jobAndTimes := make([]*JobAndTime, 0, len(jobs))
	for _, job := range jobs {
		et := predictor.PredictSolelyFastestExecutionTime(job)
		jobAndTimes = append(jobAndTimes, &JobAndTime{
			Job:  job,
			Time: et,
		})
	}
	sort.Slice(jobAndTimes, func(i, j int) bool {
		return jobAndTimes[i].Time < jobAndTimes[j].Time
	})
	result := make(map[string]int)
	for i, jat := range jobAndTimes {
		result[jat.Job.GetJobID()] = i
	}
	return result
}
