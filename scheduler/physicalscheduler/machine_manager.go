package physicalscheduler

import (
    . "github.com/xtao/bioflow/common"
)

type machineMgr interface {
    createInstances(capacity int, userId string) error
    deleteInstances([]string) error
    listInstances() (int, error)
    setConfig(config PhysicalConfig)
}

