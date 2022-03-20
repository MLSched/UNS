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
	GetResult(taskAllocation *objects.TaskAllocation) EachPredictResult
	Range(func(allocation *objects.TaskAllocation, result EachPredictResult))
	Combine(target PredictResult) PredictResult
}

type EachPredictResult interface {
	GetStartExecutionNanoTime() *int64
	GetFinishNanoTime() *int64
}

type Error struct {
	ErrorType ErrorType
	Reason    string
}

func (e *Error) Error() string {
	return e.Reason
}

func (e *Error) Set(reason string) *Error {
	err := BuildError(e.ErrorType)
	err.Reason = reason
	return err
}

func BuildError(errorType ErrorType) *Error {
	return &Error{ErrorType: errorType}
}

type ErrorType int

const (
	UnsupportedJobType           ErrorType = 0
	UnsupportedTaskGroupType     ErrorType = 1
	NonPlaceholderUnsetStartTime ErrorType = 1
	SpaceSharingOutOfMemory      ErrorType = 1
	SpaceSharingMoreThanTwo      ErrorType = 1
	SpaceSharingDiffTaskType     ErrorType = 1
	SpaceSharingDiffAccID        ErrorType = 1
	UnsupportedDLTGangType       ErrorType = 1
)

var UnsupportedJobTypeError = BuildError(UnsupportedJobType)
var UnsupportedTaskGroupTypeError = BuildError(UnsupportedTaskGroupType)
var NonPlaceholderUnsetStartTimeError = BuildError(NonPlaceholderUnsetStartTime)
var SpaceSharingOutOfMemoryError = BuildError(SpaceSharingOutOfMemory)
var SpaceSharingMoreThanTwoError = BuildError(SpaceSharingMoreThanTwo)
var SpaceSharingDiffTaskTypeError = BuildError(SpaceSharingDiffTaskType)
var SpaceSharingDiffAccIDError = BuildError(SpaceSharingDiffAccID)
var UnsupportedDLTGangTypeError = BuildError(UnsupportedDLTGangType)

func CheckErrorType(err error, errorType ErrorType) bool {
	if e, ok := err.(*Error); ok {
		return e.ErrorType == errorType
	}
	return false
}

func IsSpaceSharingOutOfMemoryError(err error) bool {
	return CheckErrorType(err, SpaceSharingOutOfMemory)
}
