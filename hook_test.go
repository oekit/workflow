package workflow

import (
	"context"
	"slices"
	"sync"
	"testing"
)

// hookEvents 记录 hook 事件序（洋葱模型用例）。
type hookEvents struct {
	mu   sync.Mutex
	list []string
}

func (e *hookEvents) add(ev string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.list = append(e.list, ev)
}

func (e *hookEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.list)
}

// sequenceHook 记录 Before/After 事件序的 hook。
type sequenceHook struct {
	name   string
	events *hookEvents
}

func (h *sequenceHook) Before(ctx context.Context, r Runnable) error {
	h.events.add(h.name + ".Before")
	return nil
}

func (h *sequenceHook) After(ctx context.Context, r Runnable, err error) {
	h.events.add(h.name + ".After")
}

// TestHook_NestedOnionSequence 集成钉（#81+#84）：父子两层、双 hook 的完整
// 事件序——每层 Before 正序、After 逆序（LIFO），洋葱逐层闭合；子流程容器
// 节点与内部步骤各触发一次（既有逐层传播语义不变）。单层的逆序/配对断言见
// workflow_test.go 的 TestHookManager_AfterReverseOrder /
// TestHookManager_AfterOnlyForHooksWithExecutedBefore。
func TestHook_NestedOnionSequence(t *testing.T) {
	events := &hookEvents{}
	hm := NewHookManager()
	hm.Add(
		&sequenceHook{name: "outer1", events: events},
		&sequenceHook{name: "outer2", events: events},
	)

	sub := NewPipe()
	sub.Add(&okTask{})
	pipe := NewPipe()
	pipe.SetHookManager(hm)
	pipe.Add(sub)

	if err := pipe.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []string{
		"outer1.Before", "outer2.Before", // 父层：sub 容器节点
		"outer1.Before", "outer2.Before", // 子层：leaf
		"outer2.After", "outer1.After", // 子层逆序闭合
		"outer2.After", "outer1.After", // 父层逆序闭合
	}
	if got := events.snapshot(); !slices.Equal(got, want) {
		t.Errorf("nested onion sequence broken:\ngot  %v\nwant %v", got, want)
	}
}

// TestHookManager_AddConcurrentWithReaders 回归测试（#99）：HookManager.Add
// 曾是无锁 append，与读侧（beforeRun/afterRun 无锁遍历 m.hooks）构成数据
// 竞争——AsyncFlow.Add 的「与执行方法并发安全」承诺让 a.Hooks().Add(h)
// 在 Run 期间也显得合法。Add 加锁 + 读侧快照后，本测试在 -race 下必须干净。
func TestHookManager_AddConcurrentWithReaders(t *testing.T) {
	hm := NewHookManager()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				hm.Add(&mockHook{})
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				_ = hm.Before(context.Background(), &okTask{})
				hm.After(context.Background(), &okTask{}, nil)
				_ = hm.Len()
			}
		}()
	}
	wg.Wait()
}

// TestAsyncFlow_HooksAddConcurrentWithRun 复现 issue #99 的原始场景：
// a.Hooks().Add(h) 与 a.Run 并发——用户从 AsyncFlow.Add 的快照承诺自然
// 推论出这一用法，此前 HookManager 无锁 append 是真数据竞争。
func TestAsyncFlow_HooksAddConcurrentWithRun(t *testing.T) {
	a := NewAsync(4)
	a.Add(&okTask{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			a.Hooks().Add(&mockHook{})
		}
	}()
	for i := 0; i < 50; i++ {
		if err := a.Run(context.Background()); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}
	<-done
}
