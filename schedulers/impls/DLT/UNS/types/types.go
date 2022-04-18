package types

import (
	"github.com/MLSched/UNS/pb_gen"
	"github.com/MLSched/UNS/pb_gen/objects"
	"github.com/MLSched/UNS/predictor/interfaces"
	benefitsinterfaces "github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"github.com/MLSched/UNS/schedulers/impls/DLT/UNS/score"
	"github.com/MLSched/UNS/schedulers/partition"
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
