package sampler

import "UNS/schedulers/impls/DLT/UNS/types"

type FixSampler struct {
	MaxCount int
}

func NewFixSampler(maxCount int) *FixSampler {
	return &FixSampler{MaxCount: maxCount}
}

func (f *FixSampler) Sample(sorted []*types.AllocContext) []*types.AllocContext {
	if len(sorted) < f.MaxCount {
		return sorted
	}
	return sorted[:f.MaxCount]
}
