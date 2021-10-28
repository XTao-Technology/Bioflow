package common

type STAGEEVENT int
type ACTION int

const (
    DATA_DONE STAGEEVENT = 0
    DATA_ABORT STAGEEVENT = 1
    DATA_SUBMIT_FAIL STAGEEVENT = 3
    DATA_LOST STAGEEVENT = 4
    DATA_FAIL STAGEEVENT = 5
    DATA_PROVISON_ACTION_NONE ACTION = 0
    DATA_PROVISON_ACTION_SCHEDULE ACTION = 1
    DATA_PROVISON_ACTION_FAIL ACTION = 2
    DATA_PROVISON_ACTION_ABORT ACTION = 3
    DATA_PROVISON_ACTION_FINISH ACTION = 4
    DATA_PROVISON_ACTION_PSEUDO_FINISH ACTION = 5
    DATA_PROVISON_ACTION_OFFLOAD_CHECK ACTION = 6
)
type DataProvider interface {
    NeedPrepare() bool
    PreHandleInput() (error, ACTION)
    GetReadyStages(stages map[string]Stage) (map[string]Stage, error)
    RestoreData() (error, ACTION)
    HandleStageEvent(stageId string, event STAGEEVENT) []string
    GetStageByID(string) Stage
    RecoverFromStage(Stage)
    CheckDataProvision() ACTION
}
