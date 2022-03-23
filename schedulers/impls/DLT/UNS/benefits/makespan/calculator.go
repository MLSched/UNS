package makespan

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/partition"
)

type Calculator struct {
}

type Stub struct {
	JobID2FinishTime map[string]int64
}

func (c *Calculator) CalIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
	s := prevStub.(*Stub)
	s = c.CloneStub(s).(*Stub)
	c.updateStub(pc, allocationsPredictResult, s)
	makeSpan := c.calculateMakeSpan(s)
	return c.makeSpan2Benefit(makeSpan), s
}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{JobID2FinishTime: make(map[string]int64)}
	oStub := stub.(*Stub)
	for jobID, finishTime := range oStub.JobID2FinishTime {
		s.JobID2FinishTime[jobID] = finishTime
	}
	return s
}

func (c *Calculator) Cal(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
	s := &Stub{JobID2FinishTime: make(map[string]int64)}
	c.updateStub(pc, allocationsPredictResult, s)
	makeSpan := c.calculateMakeSpan(s)
	return c.makeSpan2Benefit(makeSpan), s
}

func (c *Calculator) updateStub(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, stub *Stub) {
	allocationsPredictResult.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
		job := pc.GetUnfinishedJob(allocation.GetJobID())
		finishTime := *result.GetFinishNanoTime()
		stub.JobID2FinishTime[job.GetJobID()] = finishTime
	})
}

// calculateMakeSpan MakeSpan定义为：所有任务最终全部完成时，最大的任务完成时间。
func (c *Calculator) calculateMakeSpan(stub *Stub) float64 {
	maximumFinishTime := int64(-1)
	for _, finishTime := range stub.JobID2FinishTime {
		if finishTime > maximumFinishTime {
			maximumFinishTime = finishTime
		}
	}
	return float64(maximumFinishTime)
}

func (c *Calculator) makeSpan2Benefit(makeSpan float64) interfaces2.Benefit {
	return interfaces2.Benefit(-makeSpan)
}
