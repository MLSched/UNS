package interfaces

import (
	"UNS/pb_gen/objects"
	"UNS/schedulers/partition"
)

type Predictor interface {
	// Predict a set of job allocations duration seconds on a specific partition.
	// The start time of each allocation must be provided.
	Predict(partition *partition.Context, allocations []*objects.JobAllocation) (PredictResult, error)
}

type PredictResult interface {
	GetResult(allocation *objects.JobAllocation) EachPredictResult
}

type EachPredictResult interface {
	GetStartExecutionTime() float64
	GetFinishTime() float64
}
