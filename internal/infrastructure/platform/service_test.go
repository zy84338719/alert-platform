package platform

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/oekit/bootstrap/healthz"
)

// P8-P1-01 失败测试：组件 run() 期间注册的后台任务健康检查必须出现在探针里。
// 修复前 RegisterRuntimeCheck 只写入本地 map，RunServiceWith 在调用 run()
// **之前**已把 map 快照注册进 healthz —— 于是 worker 的 background-tasks
// 永远进不了 /healthz//readyz：关键后台任务退出后进程仍报健康。
// healthz 的注册表是请求时读取的，因此"晚注册"必须立刻可见。
func TestLateRuntimeCheckIsObserved(t *testing.T) {
	const name = "background-tasks-p801"
	healthz.DeregisterCheck(name)
	defer healthz.DeregisterCheck(name)

	// 模拟 cmd 在 run() 内（HTTP 服务已启动之后）注册检查。
	RegisterRuntimeCheck(name, func(context.Context) error {
		return errors.New("critical background task exited")
	})

	status, body := healthz.Handle(context.Background())
	if status != 503 {
		t.Fatalf("late runtime check not observed: status=%d body=%s", status, body)
	}
	if !strings.Contains(body, name) {
		t.Fatalf("late runtime check missing from probe body: %s", body)
	}
}

// 通过状态：后台任务全部健康时探针维持 200（不因动态注册引入误报）。
func TestLateRuntimeCheckHealthy(t *testing.T) {
	const name = "background-tasks-p801-ok"
	healthz.DeregisterCheck(name)
	defer healthz.DeregisterCheck(name)

	RegisterRuntimeCheck(name, func(context.Context) error { return nil })
	status, body := healthz.Handle(context.Background())
	if status != 200 {
		t.Fatalf("healthy background tasks must keep the probe green: status=%d body=%s", status, body)
	}
}