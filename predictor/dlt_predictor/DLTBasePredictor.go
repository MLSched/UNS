package dlt_predictor

import (
	"UNS/pb_gen/objects"
	"UNS/predictor/base"
	"UNS/schedulers/partition"
)

type DLTBasePredictor struct {
	*base.Base
}

func NewDLTBasePredictor() *DLTBasePredictor {
	return &DLTBasePredictor{
		Base: base.New([]objects.JobType{
			objects.JobType_jobTypeDLT,
		}, []objects.TaskGroupType{
			objects.TaskGroupType_taskGroupTypeSingle,
			objects.TaskGroupType_taskGroupTypeGang,
		}),
	}
}

func (p *DLTBasePredictor) PrerequisiteCheck(partitionContext *partition.Context, allocations []*objects.JobAllocation) error {
	if err := p.Base.PrerequisiteCheck(partitionContext, allocations); err != nil {
		return err
	}
	return nil
}
