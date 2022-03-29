package DDL

import (
	"UNS/pb_gen/objects"
	"UNS/schedulers/impls/DLT/UNS/benefits/base"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/partition"
	"math"
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
	JobID2VioDeadline map[string]int64
	JobID2JCT         map[string]int64
}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{
		JobID2VioDeadline: make(map[string]int64),
		JobID2JCT:         make(map[string]int64),
	}
	oStub := stub.(*Stub)
	for jobID, vioDeadline := range oStub.JobID2VioDeadline {
		s.JobID2VioDeadline[jobID] = vioDeadline
	}
	for jobID, JCT := range oStub.JobID2JCT {
		s.JobID2JCT[jobID] = JCT
	}
	return s
}

func (c *Calculator) ByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) (benefit interfaces2.Benefit, stub interface{}) {
	return 0, nil
}

func (c *Calculator) UpdateStub(pc *partition.Context, contexts map[string]*base.BenefitCalculationContext, stub interface{}) {
	s := stub.(*Stub)
	for _, ctx := range contexts {
		job := ctx.Job
		finishTime := ctx.FinishTime
		if job.GetDeadline() == math.MaxInt64 {
			s.JobID2VioDeadline[job.GetJobID()] = 0
			return
		}
		vioDeadline := finishTime - job.GetDeadline()
		s.JobID2VioDeadline[job.GetJobID()] = vioDeadline
	}
	for _, ctx := range contexts {
		job := ctx.Job
		submitTime := job.GetSubmitTimeNanoSecond()
		finishTime := ctx.FinishTime
		s.JobID2JCT[job.GetJobID()] = finishTime - submitTime
	}
}

func (c *Calculator) NewStub() interface{} {
	return &Stub{
		JobID2VioDeadline: make(map[string]int64),
		JobID2JCT:         make(map[string]int64),
	}
}

func (c *Calculator) calculateBenefit(stub *Stub) interfaces2.Benefit {
	totalVioDeadline := int64(0)
	vioDeadlineJobCount := 0
	for _, vioDeadline := range stub.JobID2VioDeadline {
		totalVioDeadline += vioDeadline
		if vioDeadline > 0 {
			vioDeadlineJobCount++
		}
	}
	totalJCT := int64(0)
	for _, JCT := range stub.JobID2JCT {
		totalJCT += JCT
	}
	benefit := -interfaces2.Benefit((1e20*float64(vioDeadlineJobCount) + float64(totalJCT)) / float64(len(stub.JobID2JCT)))
	//benefit := -interfaces2.Benefit(totalVioDeadline)
	return benefit
}

func (c *Calculator) Calculate(stub interface{}) interfaces2.Benefit {
	return c.calculateBenefit(stub.(*Stub))
}
