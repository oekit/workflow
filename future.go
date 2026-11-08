package workflow

import (
	"context"
	"sync"
	"sync/atomic"
)

// Future 异步任务的结果。
// 支持等待完成、检查状态、取消。
// Wait 可重入（#43）：结果只产生一次并被缓存，重复调用（含并发调用、
// 首次超时/取消返回后重试）都返回同一个缓存值，不会二次阻塞。
type Future struct {
	// done 在结果写入 result 之后关闭：关闭即广播，所有等待者同时被唤醒，
	// 结果不会像一次性 channel 消费那样被首个 Wait 独占。
	done   chan struct{}
	cancel context.CancelFunc
	// canceled 标记「调用方经 Future.Cancel 显式取消」（#35）：显式取消的
	// 语义是「放弃等待即成功返回」；外部 parent ctx 取消则必须上抛——
	// RunAsync 的取消兜底以本标记区分两者。
	canceled atomic.Bool

	// mu 保护 result；complete 先写 result 再 close(done)，
	// 等待者见到 done 关闭后读取 result 一定已就绪。
	mu     sync.Mutex
	result error
}

// newFuture 创建一个新的 Future。
func newFuture(cancel context.CancelFunc) *Future {
	return &Future{
		done:   make(chan struct{}),
		cancel: cancel,
	}
}

// complete 写入最终结果并广播完成（幂等：仅首次调用生效，后续调用忽略）。
// 由结果生产方在任务收尾后调用一次。
func (f *Future) complete(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	select {
	case <-f.done:
		return // 已完成：保持首个结果
	default:
	}
	f.result = err
	// 必须先写 result 再 close(done)：close 既是完成广播也是内存屏障，
	// 等待者被唤醒后读到的 result 一定已就绪——不存在 IsDone()==true
	// 但 Wait() 仍阻塞/读不到结果的窗口。
	close(f.done)
}

// cachedResult 读取缓存的结果。
// 调用前提：done 已关闭（complete 已写入 result）。
func (f *Future) cachedResult() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.result
}

// Wait 等待任务完成并返回结果。
// 如果 ctx 在结果就绪前被取消，返回 ctx.Err()。
// Wait 可重入（#43）：结果只产生一次并被缓存到字段（mutex 保护），首次
// 消费后，后续调用——包括并发调用、以及首次 Wait 超时/取消返回 ctx.Err()
// 后的重试——都直接拿到同一个缓存值，不会永久阻塞。
// 优先返回已就绪的结果（结果就绪时立即返回，即使 ctx 已取消）；ctx 与
// 结果双就绪时，非取消类的缓存错误优先于 ctx.Err()（#61）——不掷硬币，
// 无真实错误时仍返回 ctx.Err()（#35 取消不得假成功）。
func (f *Future) Wait(ctx context.Context) error {
	// 非阻塞检查：结果可能已经就绪
	select {
	case <-f.done:
		return f.cachedResult()
	default:
	}
	// 阻塞等待
	select {
	case <-ctx.Done():
		// #61：ctx 与结果双就绪时，select 的二选一是随机掷硬币——此前
		// ~29/30 返回 ctx.Err()，把任务真实错误（如 DB 约束冲突）掩蔽为
		// 「流程被取消」，与 ParallelFlow 同场景恒返回真实错误分叉。
		// 结果此时已缓存（complete 先写 result 再 close(done)）：非取消类
		// 错误优先返回——对齐文档承诺的「优先返回已就绪的结果」。无真实
		// 错误（含缓存为 nil 或取消类错误）时仍返回 ctx.Err()，不破坏 #35
		// 「取消不得假成功」。
		if f.IsDone() {
			if err := f.cachedResult(); err != nil && !isCancellationErr(err) {
				return err
			}
		}
		return ctx.Err()
	case <-f.done:
		return f.cachedResult()
	}
}

// IsDone 检查任务是否已完成。
// 返回 true 时结果已缓存，Wait 会立即返回，不再阻塞。
func (f *Future) IsDone() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}

// Cancel 取消异步任务。
// 如果任务已完成，此方法无效。
func (f *Future) Cancel() {
	f.canceled.Store(true)
	if f.cancel != nil {
		f.cancel()
	}
}
