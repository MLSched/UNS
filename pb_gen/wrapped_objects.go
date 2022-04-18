package pb_gen

import "github.com/MLSched/UNS/pb_gen/objects"

type JobAllocation struct {
	*objects.JobAllocation

	// 多余的描述性信息，且Adapter不需要知道的信息可以定义在此处。
	PlaceholderWaitingJobIDs []string
}

func WrapJobAllocation(allocation *objects.JobAllocation) *JobAllocation {
	return &JobAllocation{JobAllocation: allocation}
}

func WrapJobAllocations(allocations []*objects.JobAllocation) []*JobAllocation {
	r := make([]*JobAllocation, 0, len(allocations))
	for _, allocation := range allocations {
		r = append(r, WrapJobAllocation(allocation))
	}
	return r
}

func UnwrapJobAllocation(allocation *JobAllocation) *objects.JobAllocation {
	return allocation.JobAllocation
}

func UnwrapJobAllocations(allocations []*JobAllocation) []*objects.JobAllocation {
	r := make([]*objects.JobAllocation, 0, len(allocations))
	for _, allocation := range allocations {
		r = append(r, allocation.JobAllocation)
	}
	return r
}
