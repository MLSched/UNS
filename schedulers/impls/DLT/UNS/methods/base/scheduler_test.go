package base

import (
	"testing"
)

func TestScheduler1(t *testing.T) {
	i := []int{1, 2, 3}
	ic := make([]int, 3, 4)
	copy(ic, i)
	t.Log(ic)
	ic = append(ic, 5)
	t.Log(ic)
}
