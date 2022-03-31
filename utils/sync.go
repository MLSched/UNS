package utils

import "sync/atomic"

func NewAtomic(initValue interface{}) *atomic.Value {
	v := &atomic.Value{}
	v.Store(initValue)
	return v
}
