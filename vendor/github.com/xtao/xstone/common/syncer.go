package common

import (
    "sync"
    "time"
    "errors"
)

type TimedWaiter struct {
    fired bool
    timeout bool
    lock sync.Mutex
    waitChans []chan struct{}
}

func NewTimedWaiter() *TimedWaiter{
    return &TimedWaiter{
        fired: false,
        timeout: false,
        waitChans: make([]chan struct{}, 0),
    }
}

var (
    TW_WAIT_TIMEOUT error = errors.New("wait timeout")
)

func (waiter *TimedWaiter)Wait(timeout int) error {
    waiter.lock.Lock()
    if waiter.fired {
        waiter.lock.Unlock()
        return nil
    }
    waitChan := make(chan struct{})
    waiter.waitChans = append(waiter.waitChans, waitChan)
    waiter.lock.Unlock()

    select {
        case <- time.After(time.Duration(timeout) * time.Second):
            return TW_WAIT_TIMEOUT
        case <- waitChan:
            return nil
    }

    return errors.New("Wait failure")
}

func (waiter *TimedWaiter)Signal() {
    waiter.lock.Lock()
    waiter.fired = true
    for i := 0; i < len(waiter.waitChans); i ++ {
        close(waiter.waitChans[i])
    }
    waiter.lock.Unlock()
}
