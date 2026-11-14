// Package worker implements the background-task supervision for alert-worker
// (P1-03): every loop runs in a managed goroutine — an unexpected exit or
// panic in a CRITICAL task fails the group (fail-stop: the run function
// returns, the process exits and the platform restarts it) instead of the
// old fire-and-forget goroutine whose death was only logged while readiness
// kept reporting healthy. Non-critical failures keep the process alive but
// surface through the readiness check.
package worker

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Task is one supervised background loop.
type Task struct {
	Name string
	// Critical marks loops whose death loses correctness (processor/relay/
	// notifier/escalation): their failure stops the whole worker. Auxiliary
	// loops (health/retention) degrade readiness instead.
	Critical bool
	Run      func(ctx context.Context) error
}

type failure struct {
	name string
	err  error
}

// Group supervises the task set for one worker process. Tasks register with
// Go before Run; Run launches them and blocks until clean completion or a
// critical failure.
type Group struct {
	mu     sync.Mutex
	tasks  []Task
	fail   map[string]failure
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewGroup builds an empty supervision group.
func NewGroup() *Group {
	return &Group{fail: make(map[string]failure)}
}

// Go registers a task (must be called before Run).
func (g *Group) Go(t Task) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tasks = append(g.tasks, t)
}

// Run launches every registered task under a cancellable context and blocks:
//   - all tasks finished (their ctx was cancelled → clean shutdown): nil;
//   - a CRITICAL task exited unexpectedly (error or panic): remaining tasks
//     are cancelled via ctx and the failure is returned (fail-stop → the
//     process exits and the platform restarts the worker).
func (g *Group) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	g.cancel = cancel
	defer cancel()

	g.mu.Lock()
	tasks := append([]Task(nil), g.tasks...)
	g.mu.Unlock()

	errCh := make(chan failure, len(tasks)+1)
	for _, t := range tasks {
		g.wg.Add(1)
		go g.supervise(runCtx, t, errCh)
	}

	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-done:
			return nil
		case f := <-errCh:
			g.mu.Lock()
			if _, dup := g.fail[f.name]; dup {
				g.mu.Unlock()
				continue
			}
			g.fail[f.name] = f
			critical := false
			for _, t := range tasks {
				if t.Name == f.name {
					critical = t.Critical
				}
			}
			g.mu.Unlock()
			if critical {
				cancel()
				<-done
				return fmt.Errorf("critical background task %q exited: %v", f.name, f.err)
			}
		}
	}
}

// ReadinessCheck reports degraded readiness for any failed task (critical or
// not) — wired into /healthz + /readyz so a dead loop is visible to the
// platform instead of silently green.
func (g *Group) ReadinessCheck() func(ctx context.Context) error {
	return func(ctx context.Context) error {
		g.mu.Lock()
		defer g.mu.Unlock()
		if len(g.fail) == 0 {
			return nil
		}
		names := make([]string, 0, len(g.fail))
		for n := range g.fail {
			names = append(names, n)
		}
		sort.Strings(names)
		first := g.fail[names[0]]
		return fmt.Errorf("background task(s) %v not running (first failure: %s: %v)", names, first.name, first.err)
	}
}

// supervise runs one task, converting panics into failures (P1-03: a
// panicking goroutine previously only logged and vanished).
func (g *Group) supervise(ctx context.Context, t Task, errCh chan<- failure) {
	defer g.wg.Done()
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		return t.Run(ctx)
	}()
	if err == nil {
		return
	}
	if ctx.Err() != nil && (err == context.Canceled || ctx.Err() == context.Canceled) {
		return // clean shutdown via cancellation (fail-stop or caller cancel)
	}
	select {
	case errCh <- failure{name: t.Name, err: err}:
	default:
		// channel full: park the failure in the readiness map directly
		g.mu.Lock()
		if _, dup := g.fail[t.Name]; !dup {
			g.fail[t.Name] = failure{name: t.Name, err: err}
		}
		g.mu.Unlock()
	}
}
