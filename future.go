package workflow

import (
	"context"
	"sync/atomic"
)

// Future 异步任务的结果。
// 支持等待完成、检查状态、取消。
type Future struct {
	ch     chan error
	done   atomic.Bool
	cancel context.CancelFunc
}

// newFuture 创建一个新的 Future。
func newFuture(cancel context.CancelFunc) *Future {
	return &Future{
		ch:     make(chan error, 1),
		cancel: cancel,
	}
}

// Wait 等待任务完成并返回结果。
// 如果 ctx 被取消，返回 ctx.Err()。
// 优先读取 channel（结果就绪时立即返回），再检查 context。
func (f *Future) Wait(ctx context.Context) error {
	// 非阻塞检查：结果可能已经就绪
	select {
	case err := <-f.ch:
		return err
	default:
	}
	// 阻塞等待
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-f.ch:
		return err
	}
}

// IsDone 检查任务是否已完成。
func (f *Future) IsDone() bool {
	return f.done.Load()
}

// Cancel 取消异步任务。
// 如果任务已完成，此方法无效。
func (f *Future) Cancel() {
	if f.cancel != nil {
		f.cancel()
	}
}
