package JCT

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/partition"
	"log"
)

type Calculator struct {
}

type Stub struct {
	JobID2JCT map[string]int64
}

func (c *Calculator) CalIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
	s := prevStub.(*Stub)
	c.updateStub(pc, allocationsPredictResult, s)
	avgJCT := c.calculateAvgJCT(s)
	return c.avgJCT2Benefit(avgJCT), s
}

func (c *Calculator) Cal(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
	s := &Stub{JobID2JCT: make(map[string]int64)}

	c.updateStub(pc, allocationsPredictResult, s)
	avgJCT := c.calculateAvgJCT(s)
	return c.avgJCT2Benefit(avgJCT), s
}

func (c *Calculator) updateStub(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, stub *Stub) {
	allocationsPredictResult.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
		job := pc.GetUnfinishedJob(allocation.GetJobID())
		submitTime := job.GetSubmitTimeNanoSecond()
		if result.GetFinishNanoTime() == nil {
			log.Printf("")
		}
		finishTime := *result.GetFinishNanoTime()
		stub.JobID2JCT[job.GetJobID()] = finishTime - submitTime
	})
}

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
