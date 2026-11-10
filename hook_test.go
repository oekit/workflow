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
