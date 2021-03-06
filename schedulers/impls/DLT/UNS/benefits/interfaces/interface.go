package interfaces

import (
	"github.com/MLSched/UNS/pb_gen/objects"
	"github.com/MLSched/UNS/predictor/interfaces"
	"github.com/MLSched/UNS/schedulers/partition"
)

type Benefit float64

type Calculator interface {
	PrioritySort(pc *partition.Context, jobs map[string]*objects.Job, predictor interfaces.Predictor) map[string]int
	ByPredictIncrementally(pc *partition.Context, allocationsPredictResult interfaces.PredictResult, prevStub interface{}) (benefit Benefit, stub interface{})
	ByPredict(pc *partition.Context, allocationsPredictResult interfaces.PredictResult) (benefit Benefit, stub interface{})
	CloneStub(stub interface{}) interface{}
	NewStub() interface{}
	ByHistory(pc *partition.Context, histories map[string]*objects.JobExecutionHistory) (benefit Benefit, stub interface{})
}
