package partition

import (
	"UNS/pb_gen/objects"
	"sync"
)

type ExecutionHistoryManager struct {
	mu                     *sync.RWMutex
	jobID2ExecutionHistory map[string]*objects.JobExecutionHistory
}

func NewExecutionHistoryManager() *ExecutionHistoryManager {
	return &ExecutionHistoryManager{
		mu:                     &sync.RWMutex{},
		jobID2ExecutionHistory: make(map[string]*objects.JobExecutionHistory),
	}
}

func (m *ExecutionHistoryManager) Add(histories ...*objects.JobExecutionHistory) {
	for _, history := range histories {
		m.add(history)
	}
}

func (m *ExecutionHistoryManager) add(history *objects.JobExecutionHistory) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.jobID2ExecutionHistory[history.GetJobID()]; !ok {
		m.jobID2ExecutionHistory[history.GetJobID()] = history
		return
	}
	oh := m.jobID2ExecutionHistory[history.GetJobID()]
	newTaskExecutionHistories := make([]*objects.TaskExecutionHistory, 0, len(oh.GetTaskExecutionHistories())+len(history.GetTaskExecutionHistories()))
	executionIDs := make(map[string]bool)
	for _, th := range oh.GetTaskExecutionHistories() {
		if _, ok := executionIDs[th.GetExecutionID()]; ok {
			continue
		}
		executionIDs[th.GetExecutionID()] = true
		newTaskExecutionHistories = append(newTaskExecutionHistories, th)
	}
	m.jobID2ExecutionHistory[history.GetJobID()] = &objects.JobExecutionHistory{
		JobID:                  history.GetJobID(),
		ResourceManagerID:      history.GetResourceManagerID(),
		PartitionID:            history.GetPartitionID(),
		TaskExecutionHistories: newTaskExecutionHistories,
	}
}

func (m *ExecutionHistoryManager) Clone() *ExecutionHistoryManager {
	no := NewExecutionHistoryManager()
	hs := make([]*objects.JobExecutionHistory, 0, len(m.jobID2ExecutionHistory))
	for _, h := range m.jobID2ExecutionHistory {
		hs = append(hs, h)
	}
	no.Add(hs...)
	return no
}

func (m *ExecutionHistoryManager) Range(traverse func(history *objects.JobExecutionHistory)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, eh := range m.jobID2ExecutionHistory {
		traverse(eh)
	}
}
