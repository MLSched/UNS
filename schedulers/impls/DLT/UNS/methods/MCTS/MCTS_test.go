package MCTS

import (
	"UNS/utils"
	"context"
	"testing"
	"time"
)

func TestName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				t.Log("Done")
				return
			default:
				t.Log("pass one")
				time.Sleep(1 * time.Second)
			}
		}
	}()
	time.Sleep(5 * time.Second)
	cancel()
	time.Sleep(1 * time.Second)
}

func TestCAS(t *testing.T) {
	a := utils.NewAtomic(false)
	a.CompareAndSwap(false, true)
	go func() {
		for {
			if a.CompareAndSwap(false, true) {
				t.Log("?")
			}
		}
	}()
	time.Sleep(5 * time.Second)
}
