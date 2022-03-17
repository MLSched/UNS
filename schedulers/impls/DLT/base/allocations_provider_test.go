package base

import (
	"testing"
)

func TestAllocationsProviderImpl_PrepareAccID2SortedTaskAllocations(t *testing.T) {
	s := []int{1, 2, 4}
	insertIdx := 3
	rear := append([]int{}, s[insertIdx:]...)
	s = append(s[:insertIdx], 3)
	s = append(s, rear...)
	t.Log(s)
}
