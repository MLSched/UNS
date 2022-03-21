package sampler

import (
	"UNS/schedulers/impls/DLT/UNS/types"
)

type Sampler interface {
	Sample(sorted []*types.AllocContext) []*types.AllocContext
}
