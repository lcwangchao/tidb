package loop

import (
	"context"
	"reflect"
	"time"

	"golang.org/x/time/rate"
)

type Channel struct {
	Case    reflect.SelectCase
	Done    bool
	Process func(reflect.Value, bool)
}

type Task interface {
	Channels() []*Channel
}

type TaskFactory interface {
	NewTask() Task
}

type FuncTaskFactory func() Task

func (f FuncTaskFactory) NewTask() Task {
	return f()
}

type defaultTask struct {
	channels []*Channel
}

func newDefaultTask(ch ...*Channel) *defaultTask {
	return &defaultTask{
		channels: ch,
	}
}

func (t *defaultTask) Channels() []*Channel {
	if t == nil {
		return nil
	}
	return t.channels
}

type RChan[T any] struct {
	ch <-chan T
}

func OnReceiveChan[T any](ch <-chan T) *RChan[T] {
	return &RChan[T]{
		ch: ch,
	}
}

func (c *RChan[T]) ProcessOrOnClose(proc func(T), onClose func()) FuncTaskFactory {
	return func() Task {
		return newDefaultTask(&Channel{
			Case: reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(c.ch),
			},
			Process: func(val reflect.Value, ok bool) {
				if ok && proc != nil {
					proc(val.Interface().(T))
				}

				if !ok && onClose != nil {
					onClose()
				}
			},
		})
	}
}

func (c *RChan[T]) Process(proc func(T)) FuncTaskFactory {
	return c.ProcessOrOnClose(proc, nil)
}

type Batch[T any] struct {
	Items      []T
	IsDelay    bool
	ChanClosed bool
}

type batchProcessTask[T any] struct {
	*defaultTask
	l  *rate.Limiter
	ch <-chan T

	Batch  Batch[T]
	procFn func(Batch[T])
}

func newBatchProcessTask[T any](ch <-chan T, l *rate.Limiter, proc func(batch Batch[T])) *batchProcessTask[T] {
	task := &batchProcessTask[T]{
		ch:     ch,
		l:      l,
		procFn: proc,
	}
	task.defaultTask = newDefaultTask(
		&Channel{
			Case: reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			},
			Process: func(val reflect.Value, ok bool) {
				if ok {
					task.Batch.Items = append(task.Batch.Items, val.Interface().(T))
				} else {
					task.Batch.ChanClosed = true
				}

				if task.Batch.IsDelay {
					return
				}

				delay := task.l.Reserve().Delay()
				if delay <= 0 {
					task.batchProcess()
					return
				}

				task.Batch.IsDelay = true
				batchCh := task.channels[1]
				batchCh.Case.Chan = reflect.ValueOf(time.After(delay))
				batchCh.Done = false
			},
		},
		&Channel{
			Case: reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(context.Background().Done()),
			},
			Process: func(_ reflect.Value, _ bool) {
				task.channels[1].Done = true
				task.batchProcess()
			},
		},
	)
	return task
}

func (t *batchProcessTask[T]) batchProcess() {
	batch := t.Batch
	t.Batch.Items = batch.Items[:0]
	t.Batch.IsDelay = false
	if t.procFn != nil {
		t.procFn(batch)
	}
}

func (c *RChan[T]) BatchProcess(proc func(Batch[T]), rateLimit rate.Limit, burst int) FuncTaskFactory {
	if burst <= 0 {
		burst = 1
	}

	return func() Task {
		return newBatchProcessTask(c.ch, rate.NewLimiter(rateLimit, burst), proc)
	}
}

type SChan[T any] struct {
	ch     chan<- T
	vals   []T
	onSend func(int, T)
}

func SendChanValue[T any](ch chan<- T, vals ...T) *SChan[T] {
	return &SChan[T]{
		ch:   ch,
		vals: vals,
	}
}

func (c *SChan[T]) OnSendValue(onSend func(int, T)) *SChan[T] {
	newC := *c
	newC.onSend = onSend
	return &newC
}

func (c *SChan[T]) NewTask() Task {
	if len(c.vals) == 0 {
		return newDefaultTask()
	}
	return newSendTask(c.ch, c.vals, c.onSend)
}

type sendTask[T any] struct {
	*defaultTask
	left   []T
	onSend func(int, T)
}

func newSendTask[T any](ch chan<- T, send []T, onSend func(int, T)) *sendTask[T] {
	firstSend := send[0]
	task := &sendTask[T]{
		left: send[1:],
	}
	task.defaultTask = newDefaultTask(&Channel{
		Case: reflect.SelectCase{
			Dir:  reflect.SelectSend,
			Chan: reflect.ValueOf(ch),
			Send: reflect.ValueOf(firstSend),
		},
		Process: func(val reflect.Value, ok bool) {
			sendCh := task.channels[0]
			if len(task.left) > 0 {
				sendCh.Case.Send = reflect.ValueOf(task.left[0])
				task.left = task.left[1:]
				sendCh.Done = false
			}
			idx := len(send) - len(task.left) - 1
			onSend(idx, val.Interface().(T))
		},
	})
	return task
}

type autoRenewTask struct {
	channels   []*Channel
	chTask     Task
	notifyCh   chan struct{}
	notifyTask Task
}

func newAutoRenewTask() *autoRenewTask {
	t := &autoRenewTask{
		channels: make([]*Channel, 0, 2),
		notifyCh: make(chan struct{}, 1),
	}
	t.notify()
	return t
}

func (t *autoRenewTask) notify() {
	select {
	case t.notifyCh <- struct{}{}:
	default:
	}
}

func (t *autoRenewTask) Channels() []*Channel {
	t.channels = t.channels[:0]
	if t.chTask != nil {
		for _, c := range t.chTask.Channels() {
			t.channels = append(t.channels, c)
		}
	}
	if t.notifyTask != nil {
		for _, c := range t.notifyTask.Channels() {
			t.channels = append(t.channels, c)
		}
	}
	return t.channels
}

type AutoRenewRChan[T any] struct {
	chFn  func() (<-chan T, bool, error)
	l     rate.Limit
	burst int
}

func OnReceiveChanWithAutoRenew[T any](chFn func() (<-chan T, bool, error), l rate.Limit, burst int) *AutoRenewRChan[T] {
	if burst <= 0 {
		burst = 1
	}

	return &AutoRenewRChan[T]{
		chFn:  chFn,
		l:     l,
		burst: burst,
	}
}

func (c *AutoRenewRChan[T]) wrapTaskFactory(t *autoRenewTask, factory func(<-chan T) Task) {
	t.notifyTask = OnReceiveChan(t.notifyCh).BatchProcess(func(Batch[struct{}]) {
		ch, ok, err := c.chFn()
		if err != nil {
			t.notify()
			return
		}

		if !ok || ch == nil {
			t.chTask = nil
			t.notifyTask = nil
			return
		}

		t.chTask = factory(ch)
	}, c.l, c.burst)()
}

func (c *AutoRenewRChan[T]) ProcessOrOnClose(proc func(T), onClose func()) FuncTaskFactory {
	return func() Task {
		t := newAutoRenewTask()
		c.wrapTaskFactory(t, func(ch <-chan T) Task {
			return OnReceiveChan(ch).ProcessOrOnClose(proc, func() {
				t.notify()
				if onClose != nil {
					onClose()
				}
			})()
		})
		return t
	}
}

func (c *AutoRenewRChan[T]) Process(proc func(T)) FuncTaskFactory {
	return c.ProcessOrOnClose(proc, nil)
}

func (c *AutoRenewRChan[T]) BatchProcess(proc func(Batch[T]), l rate.Limit, burst int) FuncTaskFactory {
	return func() Task {
		t := newAutoRenewTask()
		c.wrapTaskFactory(t, func(ch <-chan T) Task {
			return OnReceiveChan(ch).BatchProcess(func(batch Batch[T]) {
				if batch.ChanClosed {
					t.notify()
				}
				if proc != nil {
					proc(batch)
				}
			}, l, burst)()
		})
		return t
	}
}
