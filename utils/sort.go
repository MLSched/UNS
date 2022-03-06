package utils

import "sort"

func SortFloat64(data []float64) {
	sorter := &Float64Sorter{Data: data}
	sort.Sort(sorter)
}

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
