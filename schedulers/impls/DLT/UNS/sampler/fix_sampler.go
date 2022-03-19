package sampler

type FixSampler struct {
	MaxCount int
}

func NewFixSampler(maxCount int) *FixSampler {
	return &FixSampler{MaxCount: maxCount}
}

func (f *FixSampler) Sample(sorted []WithBenefit) []WithBenefit {
	if len(sorted) < f.MaxCount {
		return sorted
	}
	return sorted[:f.MaxCount]
}
