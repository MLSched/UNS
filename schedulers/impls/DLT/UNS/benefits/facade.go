package benefits

import (
	"UNS/schedulers/impls/DLT/UNS/benefits/DDL"
	"UNS/schedulers/impls/DLT/UNS/benefits/JCT"
	"UNS/schedulers/impls/DLT/UNS/benefits/composite"
	"UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/impls/DLT/UNS/benefits/makespan"
)

func NewJCTCalculator() interfaces.Calculator {
	return &JCT.Calculator{}
}

func NewDDLCalculator() interfaces.Calculator {
	return &DDL.Calculator{}
}

func NewMakeSpanCalculator() interfaces.Calculator {
	return &makespan.Calculator{}
}

func NewCompositeCalculator(calculator2Coefficient map[interfaces.Calculator]float64) interfaces.Calculator {
	return &composite.Calculator{
		Calculator2Coefficient: calculator2Coefficient,
	}
}
