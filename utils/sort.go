package utils

import (
	mapset "github.com/deckarep/golang-set"
	"sort"
)

func SortFloat64(data []float64) {
	sorter := &Float64Sorter{Data: data}
	sort.Sort(sorter)
}

func SortInt64(data []int64) {
	sorter := &Int64Sorter{Data: data}
	sort.Sort(sorter)
}

func DeDuplicateSortInt64(data []int64) []int64 {
	s := mapset.NewThreadUnsafeSet()
	for _, d := range data {
		s.Add(d)
	}
	r := make([]int64, 0, len(data))
	for i := range s.Iterator().C {
		r = append(r, i.(int64))
	}
	sort.Slice(r, func(i, j int) bool {
		return r[i] < r[j]
	})
	return r
}

//func SortExecutionRanges(data []*objects.ExecutionRange) {
//	sorter := &Sorter{
//		LenFunc: func() int {
//			return len(data)
//		},
//		LessFunc: func(i, j int) bool {
//			if data[i].GetStartExecutionTimeNanoSecond() != nil && data[j].GetStartExecutionTimeNanoSecond() != nil {
//				return data[i].GetStartExecutionTimeNanoSecond().GetValue() < data[j].GetStartExecutionTimeNanoSecond().GetValue()
//			}
//			if data[i].GetStartExecutionTimeNanoSecond() == nil {
//				return false
//			}
//			return true
//		},
//		SwapFunc: func(i, j int) {
//			t := data[i]
//			data[i] = data[j]
//			data[j] = t
//		},
//	}
//	if sort.IsSorted(sorter) {
//		return
//	}
//	sort.Sort(sorter)
//}

type Sorter struct {
	LenFunc  func() int
	LessFunc func(i, j int) bool
	SwapFunc func(i, j int)
}

func (s Sorter) Len() int {
	return s.LenFunc()
}

func (s Sorter) Less(i, j int) bool {
	return s.LessFunc(i, j)
}

func (s Sorter) Swap(i, j int) {
	s.SwapFunc(i, j)
}

type Float64Sorter struct {
	Data []float64
}

func (f *Float64Sorter) Len() int {
	return len(f.Data)
}

func (f *Float64Sorter) Less(i, j int) bool {
	return f.Data[i] < f.Data[j]
}

func (f *Float64Sorter) Swap(i, j int) {
	t := f.Data[i]
	f.Data[i] = f.Data[j]
	f.Data[j] = t
}

type Int64Sorter struct {
	Data []int64
}

func (f *Int64Sorter) Len() int {
	return len(f.Data)
}

func (f *Int64Sorter) Less(i, j int) bool {
	return f.Data[i] < f.Data[j]
}

func (f *Int64Sorter) Swap(i, j int) {
	t := f.Data[i]
	f.Data[i] = f.Data[j]
	f.Data[j] = t
}

type HeapSorter struct {
	LenFunc  func() int
	LessFunc func(i, j int) bool
	SwapFunc func(i, j int)
	PushFunc func(x interface{})
	PopFunc  func() interface{}
}

func (b HeapSorter) Len() int {
	return b.LenFunc()
}

func (b HeapSorter) Less(i, j int) bool {
	return b.LessFunc(i, j)
}

func (b HeapSorter) Swap(i, j int) {
	b.SwapFunc(i, j)
}

func (b HeapSorter) Push(x interface{}) {
	b.PushFunc(x)
}

func (b HeapSorter) Pop() interface{} {
	return b.PopFunc()
}
