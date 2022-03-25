package DDL

import (
	"UNS/pb_gen/objects"
	"UNS/schedulers/impls/DLT/UNS/benefits/base"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/partition"
)

type Calculator struct {
	*base.CalculatorCommon
}

//func (c *Calculator) ByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) (benefit interfaces2.Benefit, stub interface{}) {
//	return c.Cal(pc, nil, func() map[string]*base.BenefitCalculationContext {
//		return c.ExtractContextByHistory(pc, histories)
//	})
//}

func NewCalculator() *Calculator {
	common := &base.CalculatorCommon{}
	c := &Calculator{}
	common.Impl = c
	c.CalculatorCommon = common
	return c
}

type Stub struct {
	JobID2VioDeadline map[string]int64
}

//func (c *Calculator) ByPredictIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
//	return c.Cal(pc, prevStub, func() map[string]*base.BenefitCalculationContext {
//		return c.ExtractContextByPredict(pc, allocationsPredictResult)
//	})
//}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{JobID2VioDeadline: make(map[string]int64)}
	oStub := stub.(*Stub)
	for jobID, JCT := range oStub.JobID2VioDeadline {
		s.JobID2VioDeadline[jobID] = JCT
	}
	return s
}

func (c *Calculator) ByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) (benefit interfaces2.Benefit, stub interface{}) {
	return 0, nil
}

//func (c *Calculator) ByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
//	return c.Cal(pc, nil, func() map[string]*base.BenefitCalculationContext {
//		return c.ExtractContextByPredict(pc, allocationsPredictResult)
//	})
//	//s := &Stub{
//	//	JobID2VioDeadline: make(map[string]int64),
//	//}
//	//c.updateStub(pc, allocationsPredictResult, s)
//	//totalVioDeadline := c.calculateTotalVioDeadline(s)
//	//return c.toBenefit(totalVioDeadline, c.startInstantlyCount(s)), s
//}

func (c *Calculator) UpdateStub(pc *partition.Context, contexts map[string]*base.BenefitCalculationContext, stub interface{}) {
	s := stub.(*Stub)
	for _, ctx := range contexts {
		job := ctx.Job
		finishTime := ctx.FinishTime
		if job.GetDeadline() == 0 {
			return
		}
		vioDeadline := finishTime - job.GetDeadline()
		//if vioDeadline <= 0 {
		//	s.JobID2VioDeadline[job.GetJobID()] = 0
		//	continue
		//}
		s.JobID2VioDeadline[job.GetJobID()] = vioDeadline
	}
}

//func (c *Calculator) updateStubByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, stub *Stub) {
//	if allocationsPredictResult == nil {
//		return
//	}
//	allocationsPredictResult.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
//		job := pc.GetUnfinishedJob(allocation.GetJobID())
//		finishTime := *result.GetFinishNanoTime()
//		if job.GetDeadline() == 0 {
//			return
//		}
//		vioDeadline := finishTime - job.GetDeadline()
//		if vioDeadline <= 0 {
//			return
//		}
//		stub.JobID2VioDeadline[job.GetJobID()] = vioDeadline
//	})
//}

func (c *Calculator) NewStub() interface{} {
	return &Stub{JobID2VioDeadline: make(map[string]int64)}
}

func (c *Calculator) calculateTotalVioDeadline(stub *Stub) float64 {
	totalVioDeadline := int64(0)
	for _, vioDeadline := range stub.JobID2VioDeadline {
		totalVioDeadline += vioDeadline
	}
	return float64(totalVioDeadline)
}

func (c *Calculator) toBenefit(totalVioDeadline float64) interfaces2.Benefit {
	return interfaces2.Benefit(-totalVioDeadline)
}

func (c *Calculator) Calculate(stub interface{}) interfaces2.Benefit {
	totalVioDeadline := c.calculateTotalVioDeadline(stub.(*Stub))
	return c.toBenefit(totalVioDeadline)
}
