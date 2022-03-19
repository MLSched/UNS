package sampler

type FixExponentSampler struct {
	FixCount int
}

func NewFixExponentSampler(fixCount int) *FixExponentSampler {
	return &FixExponentSampler{FixCount: fixCount}
}

func (f *FixExponentSampler) Sample(sorted []WithBenefit) []WithBenefit {
	if len(sorted) < f.FixCount {
		return sorted
	}
	base := sorted[:f.FixCount]
	currExponent := 2
	exponentIdx := f.FixCount + currExponent
	for exponentIdx < len(sorted) {
		base = append(base, sorted[exponentIdx])
		currExponent *= 2
		exponentIdx += currExponent
	}
	return base
}
