package benefits

import (
	"UNS/schedulers/impls/DLT/UNS/benefits/JCT"
	"UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
)

func NewJCTCalculator() interfaces.Calculator {
	return JCT.NewCalculator()
}

func NewDDLCalculator() interfaces.Calculator {
	panic("")
	//return DDL.NewCalculator()
}

func NewMakeSpanCalculator() interfaces.Calculator {
	panic("")
	//return makespan.NewCalculator()
}

func NewCompositeCalculator(calculator2Coefficient map[interfaces.Calculator]float64) interfaces.Calculator {
	panic("")
	//return composite.NewCalculator()
}
