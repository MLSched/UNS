package benefits

import (
	"github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/DDL"
	"github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/DDLJCT"
	"github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/JCT"
	"github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/makespan"
)

func NewJCTCalculator() interfaces.Calculator {
	return JCT.NewCalculator()
}

func NewDDLJCTCalculator() interfaces.Calculator {
	return DDLJCT.NewCalculator()
}

func NewDDLCalculator(useCountAsStandard bool) interfaces.Calculator {
	return DDL.NewCalculator(useCountAsStandard)
}

func NewMakeSpanCalculator() interfaces.Calculator {
	return makespan.NewCalculator()
}

func NewCompositeCalculator(calculator2Coefficient map[interfaces.Calculator]float64) interfaces.Calculator {
	panic("")
	//return composite.NewCalculator()
}
