package wom



type WomFlowGraphInfo struct {
    Completed bool
}

func NewWomFlowGraphInfo() *WomFlowGraphInfo {
    return &WomFlowGraphInfo {
    }
}


func (info *WomFlowGraphInfo) FlowGraphInfoToJSON() (error, string) {
    return nil, ""
}

func (info *WomFlowGraphInfo)FlowGraphInfoFromJSON(jsonData string) error {
    return nil
}

func (info *WomFlowGraphInfo)FlowGraphInfoCompleted() bool {
    return info.Completed
}

func (info *WomFlowGraphInfo)FlowGraphInfoSetCompleted(complete bool) {
    info.Completed = complete
}