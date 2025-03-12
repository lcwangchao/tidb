package loop

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
)

type loopChannel struct {
	task Task
	*Channel
}

type scheduleTaskOptions struct {
	name string
}

func loadScheduleOptions(options []ScheduleTaskOption) (opt scheduleTaskOptions) {
	for _, o := range options {
		o(&opt)
	}
	return
}

var seq atomic.Uint64

func genRandomName(prefix string) string {
	num := seq.Add(1)
	return fmt.Sprintf("%sautoname-%d", prefix, num)
}

type ScheduleTaskOption func(*scheduleTaskOptions)

func WithTaskName(name string) ScheduleTaskOption {
	return func(o *scheduleTaskOptions) {
		o.name = name
	}
}

type TaskHandler struct {
	name string
	task Task
	l    *Loop
}

func (t *TaskHandler) TaskName() string {
	return t.name
}

func (t *TaskHandler) Remove() {
	t.l.remove(t.name, t.task)
}

type Loop struct {
	ctx       context.Context
	cancel    context.CancelFunc
	runOnce   sync.Once
	notify    chan struct{}
	closeOnce sync.Once
	mu        struct {
		sync.Mutex
		tasks map[string]Task
	}
}

func NewLoop(ctx context.Context) *Loop {
	ctx, cancel := context.WithCancel(ctx)
	return &Loop{
		ctx:    ctx,
		cancel: cancel,
		notify: make(chan struct{}, 1),
	}
}

func (l *Loop) Schedule(factory TaskFactory, options ...ScheduleTaskOption) (*TaskHandler, error) {
	return l.schedule(factory, loadScheduleOptions(options))
}

func (l *Loop) schedule(factory TaskFactory, opt scheduleTaskOptions) (*TaskHandler, error) {
	if opt.name == "" {
		opt.name = genRandomName("task-")
	}
	mu := &l.mu
	mu.Lock()
	defer mu.Unlock()
	if mu.tasks == nil {
		mu.tasks = make(map[string]Task)
	}

	task := factory.NewTask()
	if _, ok := mu.tasks[opt.name]; ok {
		return nil, errors.Errorf("task '%s' already exists", opt.name)
	}

	mu.tasks[opt.name] = task
	l.notifyLoop()
	return &TaskHandler{
		name: opt.name,
		task: task,
		l:    l,
	}, nil
}

func (l *Loop) Remove(name string) (ok bool) {
	return l.remove(name, nil)
}

func (l *Loop) ListTasks() []*TaskHandler {
	l.mu.Lock()
	defer l.mu.Unlock()
	tasks := make([]*TaskHandler, 0, len(l.mu.tasks))
	for name, task := range l.mu.tasks {
		tasks = append(tasks, &TaskHandler{
			name: name,
			task: task,
			l:    l,
		})
	}
	return tasks
}

func (l *Loop) remove(name string, compare Task) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if task, ok := l.mu.tasks[name]; ok && (compare == nil || task == compare) {
		delete(l.mu.tasks, name)
		l.notifyLoop()
		return true
	}
	return false
}

func (l *Loop) After(d time.Duration, do func(time.Time), options ...ScheduleTaskOption) (h *TaskHandler, err error) {
	ch := time.After(d)
	opt := loadScheduleOptions(options)
	if opt.name == "" {
		opt.name = genRandomName(fmt.Sprintf("task-after-%s-", d))
	}
	h, err = l.schedule(OnReceiveChan(ch).Process(func(t time.Time) {
		h.Remove()
		if do != nil {
			do(t)
		}
	}), opt)
	return
}

func (l *Loop) Every(d time.Duration, do func(time.Time), options ...ScheduleTaskOption) (*TaskHandler, error) {
	ch := time.Tick(d)
	opt := loadScheduleOptions(options)
	if opt.name == "" {
		opt.name = genRandomName(fmt.Sprintf("task-every-%s-", d))
	}
	return l.schedule(OnReceiveChan(ch).Process(do), opt)
}

func (l *Loop) Instant(do func(), options ...ScheduleTaskOption) (*TaskHandler, error) {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	close(ch)
	opt := loadScheduleOptions(options)
	if opt.name == "" {
		opt.name = genRandomName("task-instant-")
	}
	return l.schedule(OnReceiveChan(ch).Process(func(struct{}) {
		if do != nil {
			do()
		}
	}), opt)
}

func (l *Loop) notifyLoop() {
	select {
	case l.notify <- struct{}{}:
	default:
	}
}

func (l *Loop) Run() {
	l.runOnce.Do(l.run)
}

func (l *Loop) run() {
	cases := []reflect.SelectCase{
		{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(l.ctx.Done()),
		},
		{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(l.notify),
		},
	}
	loopCancel, loopNotify := 0, 1
	sysCaseCnt := len(cases)
	channels := make([]loopChannel, 0)

	for {
		cases, channels = l.appendTasks(cases[:sysCaseCnt], channels[:0])
		chosen, val, ok := reflect.Select(cases)
		switch {
		case chosen == loopCancel:
			return
		case chosen == loopNotify:
			continue
		case chosen >= sysCaseCnt:
			chosen -= sysCaseCnt
		default:
			continue
		}

		ch := channels[chosen]
		if ch.Case.Dir == reflect.SelectRecv && !ok {
			ch.Done = true
		}

		if ch.Case.Dir == reflect.SelectSend {
			ch.Done = true
			val = ch.Case.Send
		}

		if ch.Process != nil {
			ch.Process(val, ok)
		}
	}
}

func (l *Loop) appendTasks(cases []reflect.SelectCase, channels []loopChannel) ([]reflect.SelectCase, []loopChannel) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for name, task := range l.mu.tasks {
		active := false
		chs := task.Channels()
		for _, ch := range chs {
			if ch.Done {
				continue
			}

			if !active {
				active = true
			}

			channels = append(channels, loopChannel{
				task:    task,
				Channel: ch,
			})
			cases = append(cases, ch.Case)
		}

		if !active {
			delete(l.mu.tasks, name)
		}
	}

	return cases, channels
}
