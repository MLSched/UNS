package utils

import (
	"testing"
)

func TestNewAtomicInt(t *testing.T) {
	a := NewAtomicInt(5)
	a.v.CompareAndSwap(5, 10)
	t.Log(a.v.Load().(int))
}
