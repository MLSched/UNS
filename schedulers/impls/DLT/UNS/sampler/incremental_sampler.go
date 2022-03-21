package sampler

import "UNS/schedulers/impls/DLT/UNS/types"

type IncrementalSampler struct {
	MaxCount        int
	EachLevelCount  int
	FirstLevelCount int
	Mask            []bool
}

func NewIncrementalSampler(maxCount int, firstLevelCount int, eachLevelCount int) *IncrementalSampler {
	sampler := &IncrementalSampler{
		MaxCount:        maxCount,
		EachLevelCount:  eachLevelCount,
		FirstLevelCount: firstLevelCount,
	}
	sampler.Mask = sampler.mask(1000000)
	return sampler
}

func (f *IncrementalSampler) Sample(sorted []*types.AllocContext) []*types.AllocContext {
	if len(sorted) < len(f.Mask) {
		return f.fastSample(sorted)
	}
	if len(sorted) < f.FirstLevelCount {
		return sorted
	}
	result := sorted[:f.FirstLevelCount]
	i := f.FirstLevelCount
	level := 2
	levelCount := 0
	for i < len(sorted) {
		result = append(result, sorted[i])
		i += level
		levelCount++
		if levelCount == f.EachLevelCount {
			levelCount = 0
			level++
		}
	}
	if len(sorted) < f.MaxCount {
		return sorted
	}
	return sorted[:f.MaxCount]
}

func (f *IncrementalSampler) fastSample(sorted []*types.AllocContext) []*types.AllocContext {
	if len(f.Mask) < len(sorted) {
		panic("fastSample mask not enough.")
	}
	result := make([]*types.AllocContext, 0, len(sorted))
	for i, w := range sorted {
		if len(result) >= f.MaxCount {
			break
		}
		if f.Mask[i] {
			result = append(result, w)
		}
	}
	return result
}

func (f *IncrementalSampler) mask(length int) []bool {
	result := make([]bool, length, length)
	for i := 0; i < f.FirstLevelCount && i < length; i++ {
		result[i] = true
	}
	i := f.FirstLevelCount
	level := 2
	levelCount := 0
	for i < length {
		result[i] = true
		i += level
		levelCount++
		if levelCount == f.EachLevelCount {
			levelCount = 0
			level++
		}
	}
	return result
}
