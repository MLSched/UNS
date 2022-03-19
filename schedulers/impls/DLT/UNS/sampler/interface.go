package sampler

import "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"

type WithBenefit interface {
	GetBenefit() interfaces.Benefit
}

type Sampler interface {
	Sample(sorted []WithBenefit) []WithBenefit
}
