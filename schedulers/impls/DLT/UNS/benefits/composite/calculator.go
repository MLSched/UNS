package composite

import (
	"UNS/predictor/interfaces"
	interfaces2 "UNS/schedulers/impls/DLT/UNS/benefits/interfaces"
	"UNS/schedulers/partition"
)

type Calculator struct {
	Calculator2Coefficient map[interfaces2.Calculator]float64
}

type Stub struct {
	Calculator2Stub map[interfaces2.Calculator]interface{}
}

func (c *Calculator) CalIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit interfaces2.Benefit, stub interface{}) {
	s := prevStub.(*Stub)
	s = c.CloneStub(s).(*Stub)
	resultBenefit := interfaces2.Benefit(0.)
	for calculator, coefficient := range c.Calculator2Coefficient {
		benefit, tempStub := calculator.CalIncrementally(pc, allocationsPredictResult, s.Calculator2Stub[calculator])
		s.Calculator2Stub[calculator] = tempStub
		resultBenefit += interfaces2.Benefit(coefficient) * benefit
	}
	return resultBenefit, s
}

func (c *Calculator) CloneStub(stub interface{}) interface{} {
	s := &Stub{Calculator2Stub: map[interfaces2.Calculator]interface{}{}}
	oStub := stub.(*Stub)
	for calculator, stub := range oStub.Calculator2Stub {
		s.Calculator2Stub[calculator] = calculator.CloneStub(stub)
	}
	return s
}

func (c *Calculator) Cal(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit interfaces2.Benefit, stub interface{}) {
	s := &Stub{
		Calculator2Stub: make(map[interfaces2.Calculator]interface{}),
	}
	resultBenefit := interfaces2.Benefit(0.)
	for calculator, coefficient := range c.Calculator2Coefficient {
		benefit, tempStub := calculator.Cal(pc, allocationsPredictResult)
		s.Calculator2Stub[calculator] = tempStub
		resultBenefit += interfaces2.Benefit(coefficient) * benefit
	}
	return resultBenefit, s
}
