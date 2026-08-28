// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"log/slog"
	"sync"
)

var (
	ErrAgentTaskAlreadyStarted  = errors.New("voice agent task has already started")
	ErrAgentTaskAlreadyComplete = errors.New("voice agent task is already complete")
	ErrAgentTaskNotComplete     = errors.New("voice agent task is not complete")
)

type AgentTaskOptions[UserData any] struct {
	AgentOptions                AgentOptions[UserData]
	PreserveFunctionCallHistory bool
}

// AgentTask is an Agent that produces exactly one typed result. Run is
// intentionally one-shot, matching Python/TypeScript foreground handoff
// semantics; Wait may be called repeatedly by independent observers.
type AgentTask[Result, UserData any] struct {
	*Agent[UserData]
	preserveFunctionCallHistory bool

	mu        sync.Mutex
	started   bool
	completed bool
	result    Result
	err       error
	done      chan struct{}
	nextID    uint64
	callbacks map[uint64]func(*AgentTask[Result, UserData])
}

func NewAgentTask[Result, UserData any](options AgentTaskOptions[UserData]) (*AgentTask[Result, UserData], error) {
	agent, err := NewAgent(options.AgentOptions)
	if err != nil {
		return nil, err
	}
	task := &AgentTask[Result, UserData]{
		Agent: agent, preserveFunctionCallHistory: options.PreserveFunctionCallHistory,
		done: make(chan struct{}), callbacks: make(map[uint64]func(*AgentTask[Result, UserData])),
	}
	agent.setTaskResultProvider(func() (any, bool, error) {
		task.mu.Lock()
		defer task.mu.Unlock()
		if !task.completed {
			return nil, false, nil
		}
		return task.result, true, task.err
	})
	return task, nil
}

func MustAgentTask[Result, UserData any](options AgentTaskOptions[UserData]) *AgentTask[Result, UserData] {
	task, err := NewAgentTask[Result](options)
	if err != nil {
		panic(err)
	}
	return task
}

func (t *AgentTask[Result, UserData]) PreserveFunctionCallHistory() bool {
	return t.preserveFunctionCallHistory
}

func (t *AgentTask[Result, UserData]) Done() bool {
	t.mu.Lock()
	done := t.completed
	t.mu.Unlock()
	return done
}

func (t *AgentTask[Result, UserData]) Complete(result Result) error {
	return t.finish(result, nil)
}

func (t *AgentTask[Result, UserData]) Fail(err error) error {
	if err == nil {
		return errors.New("agent task failure requires a non-nil error")
	}
	var zero Result
	return t.finish(zero, err)
}

func (t *AgentTask[Result, UserData]) finish(result Result, err error) error {
	t.mu.Lock()
	if t.completed {
		t.mu.Unlock()
		return ErrAgentTaskAlreadyComplete
	}
	t.completed, t.result, t.err = true, result, err
	close(t.done)
	callbacks := make([]func(*AgentTask[Result, UserData]), 0, len(t.callbacks))
	for id := uint64(0); id < t.nextID; id++ {
		if callback := t.callbacks[id]; callback != nil {
			callbacks = append(callbacks, callback)
		}
	}
	clear(t.callbacks)
	t.mu.Unlock()
	if len(callbacks) != 0 {
		go func() {
			for _, callback := range callbacks {
				invokeAgentTaskCallback(callback, t)
			}
		}()
	}
	return nil
}

func invokeAgentTaskCallback[Result, UserData any](callback func(*AgentTask[Result, UserData]), task *AgentTask[Result, UserData]) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("agent task callback panicked", "panic", recovered)
		}
	}()
	callback(task)
}

func (t *AgentTask[Result, UserData]) Result() (Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.completed {
		var zero Result
		return zero, ErrAgentTaskNotComplete
	}
	return t.result, t.err
}

func (t *AgentTask[Result, UserData]) Wait(ctx context.Context) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		var zero Result
		return zero, context.Cause(ctx)
	case <-t.done:
		return t.Result()
	}
}

func (t *AgentTask[Result, UserData]) Run(ctx context.Context) (Result, error) {
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		var zero Result
		return zero, ErrAgentTaskAlreadyStarted
	}
	t.started = true
	t.mu.Unlock()
	return t.Wait(ctx)
}

func (t *AgentTask[Result, UserData]) OnDone(callback func(*AgentTask[Result, UserData])) func() {
	if callback == nil {
		return func() {}
	}
	t.mu.Lock()
	if t.completed {
		t.mu.Unlock()
		go invokeAgentTaskCallback(callback, t)
		return func() {}
	}
	id := t.nextID
	t.nextID++
	t.callbacks[id] = callback
	t.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			delete(t.callbacks, id)
			t.mu.Unlock()
		})
	}
}
