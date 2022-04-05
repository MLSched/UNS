package utils

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

func StringSliceEquals(a []string, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i, s := range a {
		if b[i] != s {
			return false
		}
	}
	return true
}

func IndexOf(s []string, target string) int {
	for i, str := range s {
		if str == target {
			return i
		}
	}
	return -1
}

func StringSliceIndexOf(slice []string, target string) int {
	for i, s := range slice {
		if s == target {
			return i
		}
	}
	return -1
}

func StringSliceJoinWith(slice []string, s string) string {
	return fmt.Sprintf("[%s]", strings.Join(slice, s))
}

func SwapStringSlice(slice []string, i, j int) {
	o := slice[i]
	slice[i] = slice[j]
	slice[j] = o
}

func StringSliceSortBy(slice []string, targetSequence []string) {
	sorter := &Sorter{
		LenFunc: func() int {
			return len(slice)
		},
		LessFunc: func(i, j int) bool {
			return StringSliceIndexOf(targetSequence, slice[i]) < StringSliceIndexOf(targetSequence, slice[j])
		},
		SwapFunc: func(i, j int) {
			SwapStringSlice(slice, i, j)
		},
	}
	sort.Sort(sorter)
}

func IntSliceJoinWith(slice []int, s string) string {
	stringSlice := make([]string, 0, len(slice))
	for _, elem := range slice {
		stringSlice = append(stringSlice, strconv.Itoa(elem))
	}
	return StringSliceJoinWith(stringSlice, s)
}

func SumFloat64(f func(item interface{}) float64, vs ...interface{}) float64 {
	s := 0.
	for _, v := range vs {
		s += f(v)
	}
	return s
}

func SumInt64(f func(item interface{}) int64, vs ...interface{}) int64 {
	s := int64(0)
	for _, v := range vs {
		s += f(v)
	}
	return s
}

func MaxInt64Interfaces(f func(item interface{}) int64, vs ...interface{}) int64 {
	max := math.Inf(-1)
	for _, v := range vs {
		max = math.Max(max, float64(f(v)))
	}
	return int64(max)
}

func MaxInt64(vs ...int64) int64 {
	max := math.Inf(-1)
	for _, v := range vs {
		max = math.Max(max, float64(v))
	}
	return int64(max)
}

func MinInt64Interfaces(f func(item interface{}) int64, vs ...interface{}) int64 {
	min := math.Inf(1)
	for _, v := range vs {
		min = math.Min(min, float64(f(v)))
	}
	return int64(min)
}

func MinInt64(vs ...int64) int64 {
	min := math.Inf(1)
	for _, v := range vs {
		min = math.Min(min, float64(v))
	}
	return int64(min)
}

func AvgFloat64(f func(i interface{}) float64, vs ...interface{}) float64 {
	return SumFloat64(f, vs...) / float64(len(vs))
}

func SliceInsert(idx int, v interface{}, ls ...interface{}) []interface{} {
	rear := append([]interface{}{}, ls[idx:]...)
	res := append(ls[:idx], v)
	res = append(res, rear...)
	return res
}

func StringSetToSlice(set map[string]bool) []string {
	s := make([]string, 0, len(set))
	for k := range set {
		s = append(s, k)
	}
	return s
}
