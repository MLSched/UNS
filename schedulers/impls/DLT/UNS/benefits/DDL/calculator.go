package DDL

import (
	"github.com/MLSched/UNS/pb_gen/objects"
	predictorinterfaces "github.com/MLSched/UNS/predictor/interfaces"
	"github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/base"
	interfaces2 "github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
	"math"
	"sort"
)

type Calculator struct {
	*base.CalculatorCommon
	useCountAsStandard bool
}

func NewCalculator(useCountAsStandard bool) *Calculator {
	common := &base.CalculatorCommon{}
	c := &Calculator{
		useCountAsStandard: useCountAsStandard,
	}
	common.Impl = c
	c.CalculatorCommon = common
	return c
}

type Stub struct {
	JobID2JCT      map[string]int64
	JobID2Deadline map[string]int64
}

func (c *Calculator) ByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) (benefit interfaces2.Benefit, stub interface{}) {
	return 0, nil
}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{
		JobID2JCT:      make(map[string]int64),
		JobID2Deadline: make(map[string]int64),
	}
	oStub := stub.(*Stub)
	for jobID, JCT := range oStub.JobID2JCT {
		s.JobID2JCT[jobID] = JCT
		s.JobID2Deadline[jobID] = oStub.JobID2Deadline[jobID]
	}
	return s
}

func (c *Calculator) UpdateStub(pc *partition.Context, contexts map[string]*base.BenefitCalculationContext, stub interface{}) {
	s := stub.(*Stub)
	for _, ctx := range contexts {
		job := ctx.Job
		submitTime := job.GetSubmitTimeNanoSecond()
		finishTime := ctx.FinishTime
		s.JobID2JCT[job.GetJobID()] = finishTime - submitTime
		s.JobID2Deadline[job.GetJobID()] = job.GetDeadline()
	}
}

func (c *Calculator) calculateTotalDistanceToDeadline(stub *Stub) int64 {
	totalDistanceToDeadline := int64(0)
	for jobID, JCT := range stub.JobID2JCT {
		if deadline := stub.JobID2Deadline[jobID]; deadline != math.MaxInt64 {
			totalDistanceToDeadline += JCT - deadline
		}
	}
	return totalDistanceToDeadline
}

func (c *Calculator) calculateTotalViolatedCount(stub *Stub) int64 {
	count := int64(0)
	for jobID, JCT := range stub.JobID2JCT {
		if deadline := stub.JobID2Deadline[jobID]; deadline != math.MaxInt64 {
			if JCT-deadline > 0 {
				count++
			}
		}
	}
	return count
}

func (c *Calculator) calculateTotalJCT(stub *Stub) int64 {
	totalJCT := int64(0)
	for _, JCT := range stub.JobID2JCT {
		totalJCT += JCT
	}
	return totalJCT
}

func (c *Calculator) NewStub() interface{} {
	return &Stub{
		JobID2JCT:      make(map[string]int64),
		JobID2Deadline: make(map[string]int64),
	}
}

func (c *Calculator) Calculate(prevStub interface{}) interfaces2.Benefit {
	stub := prevStub.(*Stub)
	if c.useCountAsStandard {
		totalDistance2Deadline := float64(c.calculateTotalDistanceToDeadline(stub))
		benefit := -1e9 * totalDistance2Deadline
		return interfaces2.Benefit(benefit)
	} else {
		totalViolatedCount := float64(c.calculateTotalViolatedCount(stub))
		return interfaces2.Benefit(-totalViolatedCount)
	}
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
		ji := jobAndTimes[i]
		jj := jobAndTimes[j]
		if c.jobHasDeadline(ji.Job) && !c.jobHasDeadline(jj.Job) {
			return true
		} else if !c.jobHasDeadline(ji.Job) && c.jobHasDeadline(jj.Job) {
			return false
		} else if c.jobHasDeadline(ji.Job) && c.jobHasDeadline(jj.Job) {
			return ji.Job.GetDeadline() < jj.Job.GetDeadline()
		} else {
			return jobAndTimes[i].Time < jobAndTimes[j].Time
		}
	})
	result := make(map[string]int)
	for i, jat := range jobAndTimes {
		result[jat.Job.GetJobID()] = i
	}
	return result
}

func (c *Calculator) jobHasDeadline(job *objects.Job) bool {
	return job.GetDeadline() != math.MaxInt64
}
