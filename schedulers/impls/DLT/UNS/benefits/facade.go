package benefits

import (
	"UNS/schedulers/impls/DLT/UNS/benefits/DDL"
	"UNS/schedulers/impls/DLT/UNS/benefits/JCT"
	"UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
)

func NewJCTCalculator() interfaces.Calculator {
	return &JCT.Calculator{}
}

func NewDDLCalculator() interfaces.Calculator {
	return &DDL.Calculator{}
}
