package benefits

import (
	"UNS/schedulers/impls/DLT/UNS/benefits/DDL"
	"UNS/schedulers/impls/DLT/UNS/benefits/JCT"
	"UNS/schedulers/impls/DLT/UNS/benefits/composite"
	"UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/impls/DLT/UNS/benefits/makespan"
)

func NewJCTCalculator() interfaces.Calculator {
	return JCT.NewCalculator()
}

func NewDDLCalculator() interfaces.Calculator {
	return DDL.NewCalculator()
}

func NewMakeSpanCalculator() interfaces.Calculator {
	return makespan.NewCalculator()
}

func NewCompositeCalculator(calculator2Coefficient map[interfaces.Calculator]float64) interfaces.Calculator {
	return composite.NewCalculator()
}
