package simulator

import (
	"github.com/MLSched/UNS/pb_gen/objects"
	"github.com/MLSched/UNS/schedulers/partition"
	"log"
	"math"
)

type Metrics struct {
	FinishedJobs             map[string]*objects.Job
	AvgJCT                   float64
	MakeSpan                 int64
	TotalVioDeadlineDuration int64
	TotalVioDeadlineCount    int64
	TotalWithDeadlineCount   int64
}

func (m *Metrics) Analyse(pc *partition.Context) {
	m.FinishedJobs = pc.FinishedJobs
	m.CalAvgJCT(pc.ExecutionHistoryManager)
	m.CalTotalVioDeadline(pc.ExecutionHistoryManager)
	m.CalMakeSpan(pc.ExecutionHistoryManager)
	m.Print()
}

func (m *Metrics) CalAvgJCT(eh *partition.ExecutionHistoryManager) {
	totalJCT := int64(0)
	eh.Range(func(history *objects.JobExecutionHistory) {
		job := m.FinishedJobs[history.GetJobID()]
		submit := job.GetSubmitTimeNanoSecond()
		start := history.GetTaskExecutionHistories()[0].GetStartExecutionTimeNanoSecond()
		finish := history.GetTaskExecutionHistories()[0].GetStartExecutionTimeNanoSecond() + history.GetTaskExecutionHistories()[0].GetDurationNanoSecond()
		JCT := finish - submit
		totalJCT += JCT
		log.Printf("allocation %v, start %v, finish %v, duration %v, JCT = %v", history.GetTaskExecutionHistories()[0], start, finish, finish-start, JCT)
	})
	avgJCT := float64(totalJCT) / float64(len(m.FinishedJobs))
	//log.Printf("avgJCT: %f", avgJCT)
	m.AvgJCT = avgJCT
}

func (m *Metrics) CalMakeSpan(eh *partition.ExecutionHistoryManager) {
	maximumFinishTime := int64(0)
	eh.Range(func(history *objects.JobExecutionHistory) {
		finish := history.GetTaskExecutionHistories()[0].GetStartExecutionTimeNanoSecond() + history.GetTaskExecutionHistories()[0].GetDurationNanoSecond()
		if finish > maximumFinishTime {
			maximumFinishTime = finish
		}
	})
	m.MakeSpan = maximumFinishTime
}

func (m *Metrics) CalTotalVioDeadline(eh *partition.ExecutionHistoryManager) {
	totalVioDeadlineDuration := int64(0)
	totalVioDeadlineCount := int64(0)
	totalWithDeadlineCount := int64(0)
	eh.Range(func(history *objects.JobExecutionHistory) {
		job := m.FinishedJobs[history.GetJobID()]
		finish := history.GetTaskExecutionHistories()[0].GetStartExecutionTimeNanoSecond() + history.GetTaskExecutionHistories()[0].GetDurationNanoSecond()
		if job.GetDeadline() == math.MaxInt64 {
			return
		}
		totalWithDeadlineCount++
		vioDeadline := finish - job.GetDeadline()
		if vioDeadline > 0 {
			totalVioDeadlineDuration += vioDeadline
			totalVioDeadlineCount++
		}
	})
	m.TotalVioDeadlineDuration = totalVioDeadlineDuration
	m.TotalVioDeadlineCount = totalVioDeadlineCount
	m.TotalWithDeadlineCount = totalWithDeadlineCount
}

func (m *Metrics) Print() {
	log.Printf("Metrics:")
	log.Printf("FinishedJobs Count: %d", len(m.FinishedJobs))
	log.Printf("Avg JCT: %f", m.AvgJCT)
	log.Printf("Total With Deadline Count: %d", m.TotalWithDeadlineCount)
	log.Printf("Total Vio Deadline Duration: %d", m.TotalVioDeadlineDuration)
	log.Printf("Total Vio Deadline Count: %d", m.TotalVioDeadlineCount)
	log.Printf("MakeSpan: %d", m.MakeSpan)
}
