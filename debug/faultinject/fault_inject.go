package debug

import (
    "fmt"
    "runtime"
    "path/filepath"
    "strings"
    "sync"
    "errors"

    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/common"
)

const(
    BIOFLOW_FORMATSTRING string = "bioflow debug fail inject start"
)


type FaultInjectType string
type UniqueAssertWork func(formating string, args... interface{}) error

const (
    MACHINE_STAGE_CLEANUP FaultInjectType = "cleanuping"
    MACHINE_STAGE_RESTORE FaultInjectType = "restoring"
    MACHINE_STAGE_RIGHTCONTROL FaultInjectType = "rightcontrol"

    /*fault inject func type, own type need define by oneself*/
    FAULT_INJECT_PANIC string = "PANIC"
    FAULT_INJECT_ERR string = "ERROR"
    FAULT_INJECT_SUSPEND string = "SUSPEND"
)

func GetInjectKey(key string, eventType string) string {
    return fmt.Sprintf("%s-t-%s", strings.ToUpper(key),
        strings.ToUpper(eventType))
}

var GlobalFaultInjectMgr *FaultInjectMgr = nil
type FaultInjectMgr struct {
    lock sync.Mutex
    EnableAssertControl bool
    events map[string]*FaultInjectEvent
}

func (mgr *FaultInjectMgr) SetAssertControlEnable(enableAssertControl bool) {
    mgr.EnableAssertControl = enableAssertControl
}

func (mgr *FaultInjectMgr) GetAssertControlEnable() bool {
    return mgr.EnableAssertControl
}

func (mgr *FaultInjectMgr)AddEvent(key string, eventType string, action string) error {
    mgr.lock.Lock()
    defer mgr.lock.Unlock()

    event := &FaultInjectEvent{
        Id: key,
        Type: eventType,
        Msg: make(chan string, 1),
        ActionType: action,
    }
    switch strings.ToUpper(action) {
        case FAULT_INJECT_PANIC:
            event.Action = FaultInjectPanic
        case FAULT_INJECT_ERR:
            event.Action = FaultInjectError
        case FAULT_INJECT_SUSPEND:
            event.Action = FaultInjectSuspend
        default:
            return errors.New("The action is invalid")
    }
    injectKey := GetInjectKey(key, eventType)
    mgr.events[injectKey] = event

    return nil
}

func (mgr *FaultInjectMgr)RemoveEvent(key string, eventType string) error {
    mgr.lock.Lock()
    defer mgr.lock.Unlock()

    injectKey := GetInjectKey(key, eventType)
    if _, found := mgr.events[injectKey]; found {
        delete(mgr.events, injectKey)
    }

    return nil
}

func (mgr *FaultInjectMgr)FindEvent(key string, eventType string) *FaultInjectEvent {
    mgr.lock.Lock()
    defer mgr.lock.Unlock()

    injectKey := GetInjectKey(key, eventType)
    return mgr.events[injectKey]
}

func (mgr *FaultInjectMgr)ResumeEvent(key string, eventType string, msg string) error {
    faultEvent := mgr.FindEvent(key, eventType)
    if faultEvent != nil {
        faultEvent.Msg <- msg
        return nil
    } else {
        return errors.New("no event matched")
    }
}

func (mgr *FaultInjectMgr)ListEvents() []FaultEvent {
    mgr.lock.Lock()
    defer mgr.lock.Unlock()

    var events []FaultEvent = nil
    for _, injectedEvent := range mgr.events {
        event := FaultEvent{
            Id: injectedEvent.Id,
            Type: injectedEvent.Type,
            Action: injectedEvent.ActionType,
        }
        events = append(events, event)
    }

    return events
}

func GetFaultInjectMgr() *FaultInjectMgr {
    if GlobalFaultInjectMgr == nil {
        GlobalFaultInjectMgr = &FaultInjectMgr{
            EnableAssertControl: false,
            events: make(map[string]*FaultInjectEvent),
        }
    }

    return GlobalFaultInjectMgr
}

type FaultFunc func(*FaultInjectEvent)error
type FaultInjectEvent struct {
    Id string
    Type string
    Action FaultFunc
    ActionType string
    Msg chan string
}

func (faultInject *FaultInjectEvent) SetId(id string) {
    faultInject.Id = id
}

func (faultInject *FaultInjectEvent) GetKey() string {
    return faultInject.Id
}

func (faultInject *FaultInjectEvent) SetType(ownType string) {
    faultInject.Type = ownType
}

func (faultInject *FaultInjectEvent) GetType() string {
    return faultInject.Type
}

/*
 * Called to trigger inject a fault
 */
func XtDebugAssert(key string, eventType string) error {
    faultMgr := GetFaultInjectMgr()
    if !faultMgr.GetAssertControlEnable() {
        return nil
    }

    /*try to match a fault*/
    faultEvent := faultMgr.FindEvent(key, eventType)
    if faultEvent == nil || faultEvent.Action == nil {
        return nil
    }
    SchedulerLogger.Debugf("Assert(%s/%s) action start\n", key, eventType)
    var err error = nil
    assertFunc := faultEvent.Action
    if assertFunc != nil {
        err = assertFunc(faultEvent)
    }
    SchedulerLogger.Debugf("Assert(%s/%s) action end\n", key, eventType)

    return err
}

func FaultInjectError(event *FaultInjectEvent) error {
    filename, line, funcname := "???", 0, "???"
    pc, filename, line, ok := runtime.Caller(2)
    if ok {
        funcname = runtime.FuncForPC(pc).Name()
        funcname = filepath.Ext(funcname)
        funcname = strings.TrimPrefix(funcname, ".")
        funcname = strings.TrimPrefix(funcname, ".")
    }

    SchedulerLogger.Infof("Injected Error(%s/%s): %s:%d:%s\n", event.Id,
        event.Type, filename, line, funcname)

    return errors.New("injected error")
}

func FaultInjectPanic(event *FaultInjectEvent) error {
    filename, line, funcname := "???", 0, "???"
    pc, filename, line, ok := runtime.Caller(2)
    if ok {
        funcname = runtime.FuncForPC(pc).Name()
        funcname = filepath.Ext(funcname)
        funcname = strings.TrimPrefix(funcname, ".")
        funcname = strings.TrimPrefix(funcname, ".")
    }

    SchedulerLogger.Infof("Injected Panic(%s/%s): %s:%d:%s\n", event.Id,
        event.Type, filename, line, funcname)

    panic("BioflowDebug")
}

func FaultInjectSuspend(event *FaultInjectEvent) error {
    filename, line, funcname := "???", 0, "???"
    pc, filename, line, ok := runtime.Caller(2)
    if ok {
        funcname = runtime.FuncForPC(pc).Name()
        funcname = filepath.Ext(funcname)
        funcname = strings.TrimPrefix(funcname, ".")
        funcname = strings.TrimPrefix(funcname, ".")
    }

    SchedulerLogger.Infof("Injected Suspend(%s/%s): %s:%d:%s\n", event.Id,
        event.Type, filename, line, funcname)

    /*suspend by wait for a message*/
    msg := <- event.Msg

    SchedulerLogger.Infof("Injected Resume(%s/%s): %s\n", event.Id,
        event.Type, msg)

    return nil
}
