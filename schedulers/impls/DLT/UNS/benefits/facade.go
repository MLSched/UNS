package benefits

import (
	"UNS/schedulers/impls/DLT/UNS/benefits/JCT"
	"UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
)

func NewJCTCalculator() interfaces.Calculator {
	return &JCT.Calculator{}
}
