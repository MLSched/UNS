package composite

import (
	"github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/base"
	interfaces2 "github.com/MLSched/UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
)

type Calculator struct {
	Calculator2Coefficient map[interfaces2.Calculator]float64
	*base.CalculatorCommon
}

func NewCalculator() *Calculator {
	common := &base.CalculatorCommon{}
	c := &Calculator{}
	common.Impl = c
	c.CalculatorCommon = common
	return c
}

func (c *Calculator) Calculate(stub interface{}) interfaces2.Benefit {
	s := stub.(*Stub)
	resultBenefit := interfaces2.Benefit(0.)
	for calculator, coefficient := range c.Calculator2Coefficient {
		benefit := calculator.(base.CalculatorTemplate).Calculate(s.Calculator2Stub[calculator])
		resultBenefit += interfaces2.Benefit(coefficient) * benefit
	}
	return resultBenefit
}

func (c *Calculator) NewStub() interface{} {
	return &Stub{Calculator2Stub: make(map[interfaces2.Calculator]interface{})}
}

func (c *Calculator) UpdateStub(pc *partition.Context, contexts map[string]*base.BenefitCalculationContext, stub interface{}) {
	s := stub.(*Stub)
	for calculator, calculatorStub := range s.Calculator2Stub {
		calculator.(base.CalculatorTemplate).UpdateStub(pc, contexts, calculatorStub)
	}
}

type Stub struct {
	Calculator2Stub map[interfaces2.Calculator]interface{}
}

//func (c *Calculator) ByPredictIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
//	s := prevStub.(*Stub)
//	s = c.CloneStub(s).(*Stub)
//	resultBenefit := interfaces2.Benefit(0.)
//	for calculator, coefficient := range c.Calculator2Coefficient {
//		benefit, tempStub := calculator.ByPredictIncrementally(pc, allocationsPredictResult, s.Calculator2Stub[calculator])
//		s.Calculator2Stub[calculator] = tempStub
//		resultBenefit += interfaces2.Benefit(coefficient) * benefit
//	}
//	return resultBenefit, s
//}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{Calculator2Stub: map[interfaces2.Calculator]interface{}{}}
	oStub := stub.(*Stub)
	for calculator, stub := range oStub.Calculator2Stub {
		s.Calculator2Stub[calculator] = calculator.CloneStub(stub)
	}
	return s
}

//
//func (c *Calculator) ByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
//	s := &Stub{
//		Calculator2Stub: make(map[interfaces2.Calculator]interface{}),
//	}
//	resultBenefit := interfaces2.Benefit(0.)
//	for calculator, coefficient := range c.Calculator2Coefficient {
//		benefit, tempStub := calculator.ByPredict(pc, allocationsPredictResult)
//		s.Calculator2Stub[calculator] = tempStub
//		resultBenefit += interfaces2.Benefit(coefficient) * benefit
//	}
//	return resultBenefit, s
//}
