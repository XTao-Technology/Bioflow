package common




type AsyncOffloadMgr interface {
    GetReadyStage() (map[string]Stage, bool, error)
    GenereateCleanupOffloadStage() (error, ACTION)
    GenereateRightControlOffloadStage(bool) (error, ACTION)
    HandleStageEvent(string, STAGEEVENT)
    GetStageByStageId(string) Stage
}