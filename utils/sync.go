package utils

import "sync/atomic"

func NewAtomic(initValue interface{}) *atomic.Value {
	v := &atomic.Value{}
	v.Store(initValue)
	return v
}

type AtomicInt struct {
	v *atomic.Value
}

func NewAtomicInt(initValue int) *AtomicInt {
	a := NewAtomic(initValue)
	return &AtomicInt{v: a}
}

func (a *AtomicInt) GetAndIncrement(delta int) int {
	for {
		v := a.v.Load().(int)
		if a.v.CompareAndSwap(v, v+delta) {
			return v
		}
	}
}

func (a *AtomicInt) Get() int {
	return a.v.Load().(int)
}
