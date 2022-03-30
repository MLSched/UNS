package types

import (
	"UNS/pb_gen"
	"UNS/pb_gen/objects"
	"UNS/predictor/interfaces"
	benefitsinterfaces "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/impls/DLT/UNS/score"
	"UNS/schedulers/partition"
)

type AllocContext struct {
	PC                           *partition.Context
	Job                          *objects.Job
	JobAllocation                *pb_gen.JobAllocation
	NewJobAllocations            []*pb_gen.JobAllocation
	NewJobAllocationsFingerPrint string
	Benefit                      benefitsinterfaces.Benefit
	BenefitStub                  interface{}
	PredictResult                interfaces.PredictResult
	Score                        score.JobAllocationsScore
}

func (j *AllocContext) GetBenefit() benefitsinterfaces.Benefit {
	return j.Benefit
}

func (j *AllocContext) GetScore() score.JobAllocationsScore {
	return j.Score
}
