package utils

import "sync/atomic"

func NewAtomic(initValue interface{}) *atomic.Value {
	v := &atomic.Value{}
	v.Store(initValue)
	return v
}

type AtomicInt struct {
	atomic.Value
}

func NewAtomicInt(initValue int) *AtomicInt {
	a := NewAtomic(initValue)
	return &AtomicInt{*a}
}

func (a *AtomicInt) GetAndIncrement(delta int) int {
	for {
		v := a.Load().(int)
		if a.CompareAndSwap(v, v+delta) {
			return v
		}
	}
}

func (a *AtomicInt) Get() int {
	return a.Load().(int)
}
