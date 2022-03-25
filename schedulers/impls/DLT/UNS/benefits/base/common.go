package base

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/partition"
)

type CalculatorCommon struct {
	Impl CalculatorTemplate
}

type CalculatorTemplate interface {
	interfaces2.Calculator

	Calculate(stub interface{}) interfaces2.Benefit
	NewStub() interface{}
	CloneStub(stub interface{}) interface{}
	UpdateStub(pc *partition.Context, contexts map[string]*BenefitCalculationContext, stub interface{})
}

type BenefitCalculationContext struct {
	Job        *objects.Job
	StartTime  int64
	FinishTime int64
}

func (c *CalculatorCommon) calculate(pc *partition.Context, prevStub interface{}, contexts map[string]*BenefitCalculationContext) (interfaces2.Benefit, interface{}) {
	var s interface{}
	if prevStub == nil {
		s = c.Impl.NewStub()
	} else {
		s = c.Impl.CloneStub(prevStub)
	}
	c.Impl.UpdateStub(pc, contexts, s)
	benefit := c.Impl.Calculate(s)
	return benefit, s
}

func (c *CalculatorCommon) ByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) (benefit interfaces2.Benefit, stub interface{}) {
	return c.calculate(pc, nil, c.ExtractContextByHistory(pc, histories))
}

func (c *CalculatorCommon) ByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
	return c.calculate(pc, nil, c.ExtractContextByPredict(pc, allocationsPredictResult))
}

func (c *CalculatorCommon) ByPredictIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
	return c.calculate(pc, prevStub, c.ExtractContextByPredict(pc, allocationsPredictResult))
}

func (c *CalculatorCommon) ExtractContextByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) map[string]*BenefitCalculationContext {
	jobID2Context := make(map[string]*BenefitCalculationContext)
	allocationsPredictResult.Range(func(allocation *objects.TaskAllocation, result interfaces.EachPredictResult) {
		jobID := allocation.GetJobID()
		if _, ok := jobID2Context[jobID]; ok {
			return
		}
		job := pc.GetJob(jobID)
		if job == nil {
			return
		}
		jobID2Context[jobID] = &BenefitCalculationContext{
			Job:        job,
			StartTime:  *result.GetStartExecutionNanoTime(),
			FinishTime: *result.GetFinishNanoTime(),
		}
	})
	return jobID2Context
}

func (c *CalculatorCommon) ExtractContextByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) map[string]*BenefitCalculationContext {
	jobID2Context := make(map[string]*BenefitCalculationContext)
	if histories == nil {
		return jobID2Context
	}
	for _, history := range histories {
		for _, taskHistory := range history.GetTaskExecutionHistories() {
			if !taskHistory.GetFinished() {
				continue
			}
			if _, ok := jobID2Context[taskHistory.GetJobID()]; ok {
				continue
			}
			job := pc.GetJob(taskHistory.GetJobID())
			if job == nil {
				continue
			}
			jobID2Context[job.GetJobID()] = &BenefitCalculationContext{
				Job:        job,
				StartTime:  taskHistory.GetStartExecutionTimeNanoSecond(),
				FinishTime: taskHistory.GetStartExecutionTimeNanoSecond() + taskHistory.GetDurationNanoSecond(),
			}
		}
	}
	return jobID2Context
}
