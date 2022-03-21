package score

import (
	"UNS/pb_gen/objects"
	"UNS/schedulers/partition"
)

type Score float64

type Calculator interface {
	GetScore(pc *partition.Context, jobAllocations []*objects.JobAllocation) (Score, interface{})
	GetScoreIncrementally(pc *partition.Context, jobAllocations []*objects.JobAllocation, stub interface{}) (Score, interface{})
}

type ConsolidationScoreCalculator struct {
}

func NewConsolidationScoreCalculator() *ConsolidationScoreCalculator {
	return &ConsolidationScoreCalculator{}
}

type ConsolidationScoreStub struct {
	NodeIDs   map[string]bool
	SocketIDs map[string]bool
}

func (c *ConsolidationScoreCalculator) GetScore(pc *partition.Context, jobAllocations []*objects.JobAllocation) (Score, interface{}) {
	stub := &ConsolidationScoreStub{
		NodeIDs:   make(map[string]bool),
		SocketIDs: make(map[string]bool),
	}
	c.updateStub(pc, jobAllocations, stub)
	return c.getScore(stub), stub
}

func (c *ConsolidationScoreCalculator) GetScoreIncrementally(pc *partition.Context, jobAllocations []*objects.JobAllocation, stub interface{}) (Score, interface{}) {
	s := stub.(*ConsolidationScoreStub)
	c.updateStub(pc, jobAllocations, s)
	return c.getScore(s), s
}

func (c *ConsolidationScoreCalculator) updateStub(pc *partition.Context, jobAllocations []*objects.JobAllocation, stub *ConsolidationScoreStub) {
	for _, jobAllocation := range jobAllocations {
		for _, taskAllocation := range jobAllocation.GetTaskAllocations() {
			accID := taskAllocation.GetAcceleratorAllocation().GetAcceleratorID()
			stub.NodeIDs[pc.View.AcceleratorID2NodeID[accID]] = true
			stub.SocketIDs[pc.View.AcceleratorID2SocketID[accID]] = true
		}
	}
}

func (c *ConsolidationScoreCalculator) getScore(stub *ConsolidationScoreStub) Score {
	s := Score(0.)
	s += Score(-10 * len(stub.NodeIDs))
	s += Score(-1 * len(stub.SocketIDs))
	return s
}
