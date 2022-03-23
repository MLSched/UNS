package interfaces

import (
	"UNS/predictor/interfaces"
	"UNS/schedulers/partition"
)

type Benefit float64

type Calculator interface {
	CalIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit Benefit, stub interface{})
	Cal(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit Benefit, stub interface{})
	CloneStub(stub interface{}) interface{}
}
