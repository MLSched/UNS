package types

import (
	"UNS/pb_gen/objects"
	predictorinterfaces "UNS/predictor/interfaces"
	benefitsinterfaces "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/impls/DLT/UNS/score"
	"UNS/schedulers/partition"
)

type AllocContext struct {
	PC                           *partition.Context
	Job                          *objects.Job
	JobAllocation                *objects.JobAllocation
	NewJobAllocations            []*objects.JobAllocation
	NewJobAllocationsFingerPrint string
	Benefit                      benefitsinterfaces.Benefit
	Score                        score.Score
	PredictResult                predictorinterfaces.PredictResult
	BenefitStub                  interface{}
	ScoreStub                    interface{}
}

func (j *AllocContext) GetBenefit() benefitsinterfaces.Benefit {
	return j.Benefit
}

func (j *AllocContext) GetScore() score.Score {
	return j.Score
}
