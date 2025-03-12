package loop

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLoop(t *testing.T) {
	l := NewLoop(context.TODO())

	instant, err := l.Instant(func() {
		log.Info("Instant")
	})
	require.NoError(t, err)
	log.Info("InstantTaskName", zap.String("name", instant.TaskName()))

	after, err := l.After(5*time.Second, func(t time.Time) {
		log.Info("After", zap.Time("time", t))
	})
	require.NoError(t, err)
	log.Info("AfterTaskName", zap.String("name", after.TaskName()))

	every, err := l.Every(10*time.Second, func(t time.Time) {
		tasks := l.ListTasks()
		names := make([]string, 0, len(tasks))
		for _, task := range tasks {
			names = append(names, task.TaskName())
		}
		log.Info("Every", zap.Time("time", t), zap.Strings("tasks", names))
	})
	require.NoError(t, err)
	log.Info("EveryTaskName", zap.String("name", every.TaskName()))

	var ch chan string
	valIdx := 0
	renewCnt := 0
	renewCh := func() (<-chan string, bool, error) {
		renewCnt++
		log.Info("renewCh", zap.Int("renewCnt", renewCnt))
		require.Nil(t, ch)
		if renewCnt > 2 && renewCnt <= 4 {
			log.Info("renewCh error")
			return nil, false, errors.New("renewCh error")
		}

		if renewCnt >= 6 {
			log.Info("stop renew ch")
			return nil, false, nil
		}

		ch = make(chan string, 1)
		return ch, true, nil
	}

	sendHandle, err := l.Every(time.Duration(500)*time.Millisecond, func(time.Time) {
		if ch == nil {
			return
		}

		valIdx++
		sendVal := fmt.Sprintf("val_%d", valIdx)
		_, err2 := l.Schedule(SendChanValue(ch, sendVal).OnSendValue(func(_ int, val string) {
			log.Info("SendChanValue.OnSendValue", zap.String("val", val))
			if valIdx%5 == 0 {
				close(ch)
				ch = nil
			}
		}))
		require.NoError(t, err2)
	}, WithTaskName("send-value"))
	require.NoError(t, err)
	log.Info("SendTaskName", zap.String("name", sendHandle.TaskName()))

	recvHandle, err := l.Schedule(OnReceiveChanWithAutoRenew(renewCh, 0.2, 1).Process(func(val string) {
		log.Info("OnReceiveChanWithAutoRenew.Process", zap.String("val", val))
	}))
	require.NoError(t, err)
	log.Info("RecvTaskName", zap.String("name", recvHandle.TaskName()))

	l.Run()
}
