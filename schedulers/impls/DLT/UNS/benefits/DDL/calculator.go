package DDL

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/partition"
)

type Calculator struct {
}

type Stub struct {
	JobID2VioDeadline map[string]int64
	StartInstantly    map[string]bool
}

func (c *Calculator) CalIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
	s := prevStub.(*Stub)
	s = c.CloneStub(s).(*Stub)
	c.updateStub(pc, allocationsPredictResult, s)
	totalVioDeadline := c.calculateTotalVioDeadline(s)
	return c.toBenefit(totalVioDeadline, c.startInstantlyCount(s)), s
}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{JobID2VioDeadline: make(map[string]int64), StartInstantly: make(map[string]bool)}
	oStub := stub.(*Stub)
	for jobID, JCT := range oStub.JobID2VioDeadline {
		s.JobID2VioDeadline[jobID] = JCT
		s.StartInstantly[jobID] = oStub.StartInstantly[jobID]
	}
	return s
}

func (c *Calculator) Cal(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
	s := &Stub{
		JobID2VioDeadline: make(map[string]int64),
		StartInstantly:    make(map[string]bool),
	}
	c.updateStub(pc, allocationsPredictResult, s)
	totalVioDeadline := c.calculateTotalVioDeadline(s)
	return c.toBenefit(totalVioDeadline, c.startInstantlyCount(s)), s
}

func (c *Calculator) updateStub(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, stub *Stub) {
	allocationsPredictResult.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
		job := pc.GetUnfinishedJob(allocation.GetJobID())
		finishTime := *result.GetFinishNanoTime()
		if allocation.GetAllocationTimeNanoSecond() == pc.FixedNow() {
			stub.StartInstantly[job.GetJobID()] = true
		}
		if job.GetDeadline() == 0 {
			return
		}
		vioDeadline := finishTime - job.GetDeadline()
		if vioDeadline <= 0 {
			return
		}
		stub.JobID2VioDeadline[job.GetJobID()] = vioDeadline
	})
}

func (c *Calculator) calculateTotalVioDeadline(stub *Stub) float64 {
	totalVioDeadline := int64(0)
	for _, vioDeadline := range stub.JobID2VioDeadline {
		totalVioDeadline += vioDeadline
	}
	return float64(totalVioDeadline)
}

func (c *Calculator) startInstantlyCount(stub *Stub) int64 {
	return int64(len(stub.StartInstantly))
}

func (c *Calculator) toBenefit(totalVioDeadline float64, startInstantlyCount int64) interfaces2.Benefit {
	//return interfaces2.Benefit((-totalVioDeadline) + float64(startInstantlyCount))
	return interfaces2.Benefit(-totalVioDeadline)
}
