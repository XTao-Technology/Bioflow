package message

type FaultEvent struct {
    Id string
    Type string
    Action string
}

type BioflowShowFaultInjectResult struct {
    Status string
    Msg string
    Enabled bool
    Events []FaultEvent
}

const (
    CMD_ENABLE_FAULT_INJECT string = "ENABLE_FAULT_INJECT"
    CMD_ADD_FAULT_EVENT string = "ADD_FAULT_EVENT"
    CMD_DELETE_FAULT_EVENT string = "DELETE_FAULT_EVENT"
    CMD_RESUME_FAULT_EVENT string = "RESUME_FAULT_EVENT"
)

type FaultInjectOpt struct {
    Cmd string
    Id string
    Type string
    Action string
    Msg string
    Enable bool
}
