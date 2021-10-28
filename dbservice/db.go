/* 
 Copyright (c) 2016-2017 XTAO technology <www.xtaotech.com>
 All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:
  1. Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
 
  THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
  OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
  HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
  SUCH DAMAGE.
*/
package dbservice

import (
    "database/sql"
    _"github.com/lib/pq"
    "fmt"
	"time"
	"sync/atomic"
    "strings"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
)

type dbStat struct {
	lastErr error
	lastErrTime time.Time
	lastErrType string
	queryErr int64
	execErr int64
}

type dbService struct {
    server string
    port string
    user string
    pass string
    dbname string
    driver string
    db *sql.DB
	stat *dbStat
}

func GetDBService() *dbService {
    return globalDBService
}

var globalDBService *dbService = nil

var dbReconnectPeriod = time.Second * 5
var dbReconnectCount = 3

func NewDBService(config *DBServiceConfig) *dbService {
	stat := &dbStat {
		queryErr: 0,
		execErr: 0,
	}

    dbService := &dbService{
		server: config.Server,
		port: config.Port,
		user: config.User,
		pass: config.Password,
		dbname: config.DBName,
		driver: config.Driver,
		stat: stat,
	}

    dbOpts := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", dbService.user,
        dbService.pass, dbService.dbname)

	DBLogger.Errorf("%v", config)

    if dbService.server != "" {
        dbOpts += " host=" + dbService.server
    }
    if dbService.port != "" {
        dbOpts += " port=" + dbService.port
    }
    db, err := sql.Open(dbService.driver, dbOpts)
    if err != nil {
        DBLogger.Errorf("Fail to open database, error %s \n", err.Error())
        return nil
    }
    dbService.db = db

	/*
     * DB service blocking retry dbReconnectCount * dbReconnectPeriod
     * until connect successfully or give up.
     */
	connected := false
    for i:=0; i <= dbReconnectCount; i++ {
		err = db.Ping()
		if err != nil {
			DBLogger.Errorf("database connection failed, retry...")
			time.Sleep(dbReconnectPeriod)
		} else {
			DBLogger.Infof("database connection successfully! \n")
			connected = true
			
			break
		}
	}

	if connected == false {
		DBLogger.Infof("give up to connect database, move on...! \n")
	}

    globalDBService = dbService

    return dbService
}

type JobDBInfo struct {
    Id string
    SecID string
    Run int
    Name string
    Description string
    Json string
    Created string
    Finished string
    State string
    PausedState string
    Pipeline string
    WorkDir string
    LogDir string
    SMID string
    RetryLimit int
    FailReason string
    Priority int
    AuxInfo string
}

func (db *dbService) ExecErr(err error) {
	db.stat.lastErrTime = time.Now()
	db.stat.lastErrType = "Exec"
	db.stat.lastErr = err
	atomic.AddInt64(&db.stat.execErr, 1)
}

func (db *dbService) QueryErr(err error) {
	db.stat.lastErrTime = time.Now()
	db.stat.lastErrType = "Query"
	db.stat.lastErr = err
	atomic.AddInt64(&db.stat.queryErr, 1)
}

func (db *dbService) Query(query string, args ...interface{}) (*sql.Rows, error) {
	var err error
	var rows *sql.Rows
	for i := 0; i <= dbReconnectCount; i++ {
		rows, err = db.db.Query(query, args ...)
		if err != nil {
			db.QueryErr(err)
			if IsConnectionError(err) != true {
				return nil, err
			} else {
				DBLogger.Errorf("database connection failed, retry...")
				time.Sleep(dbReconnectPeriod)
			}
		} else {
			break
		}
	}
	return rows, err
}

func (db *dbService) Exec(sqlStr string, args ...interface{}) error {
    stmt, err := db.db.Prepare(sqlStr)
	if err != nil {
        DBLogger.Errorf("DBService: prepare error %s\n", err.Error())
        return err
    }

    defer stmt.Close()

	for i := 0; i <= dbReconnectCount; i++ {
		_, err = stmt.Exec(args ...)
		if err != nil {
			db.ExecErr(err)
			if IsConnectionError(err) != true {
				break
			} else {
				DBLogger.Errorf("database connection failed, retry...")
				time.Sleep(dbReconnectPeriod)
			}
		} else {
			break
		}
	}
	return err
}

func (db *dbService) TxExec(tx *sql.Tx,  sqlStr string, args ...interface{}) error {
	var err error = nil

	for i := 0; i <= dbReconnectCount; i++ {
		_, err = tx.Exec(sqlStr, args ...)
		if err != nil {
			db.ExecErr(err)
			if IsConnectionError(err) != true {
				break
			} else {
				DBLogger.Errorf("database connection failed, retry...")
				time.Sleep(dbReconnectPeriod)
			}
		} else {
			break
		}
	}
	return err
}

func (db *dbService) AddJob(jobInfo *JobDBInfo) error {
    insertSql := "INSERT INTO JobInfo(id,run,name,description,json,created,finished,"
    insertSql += "state,pipeline,workdir,logdir,smid,retrylimit,pausedstate,secid,auxinfo,priority) "
    insertSql += " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)"

    err := db.Exec(insertSql, jobInfo.Id, jobInfo.Run, jobInfo.Name, jobInfo.Description,
		jobInfo.Json, jobInfo.Created, jobInfo.Finished, jobInfo.State, jobInfo.Pipeline,
		jobInfo.WorkDir, jobInfo.LogDir, jobInfo.SMID, jobInfo.RetryLimit, jobInfo.PausedState,
        jobInfo.SecID, jobInfo.AuxInfo, jobInfo.Priority)

    if err != nil {
        DBLogger.Errorf("Fail to add job id %s name %s to database\n",
            jobInfo.Id, jobInfo.Name)
    } else {
        DBLogger.Infof("Succeed to add job id %s name %s to database\n",
            jobInfo.Id, jobInfo.Name)
    }

    return err
}

func (db *dbService) MovJobSchedHisToJobScheduleDB(schedInfo *JobScheduleDBInfo) error {
    err := db.AddJobScheduleInfo(schedInfo)
    if err != nil {
        DBLogger.Errorf("Mov jobId:%s jobSchedulerHis to JobScheduleDB failed:%s\n",
            schedInfo.Id, err.Error())
    }

    return err
}

func (db *dbService) MovJobHisToJob(jobInfo *JobDBInfo) error {
	var err error
	err = nil

	tx, err := db.db.Begin()
	defer func() {
		if tx != nil {
			if err != nil {
				tx.Rollback()
			} else {
				tx.Commit()
			}
		}
	}()

	deleteSql := "DELETE FROM JobHistory WHERE id=$1"

    err = db.TxExec(tx, deleteSql, jobInfo.Id)
	if err != nil {
        DBLogger.Errorf("Fail to delete job %s history from database %s\n",
            jobInfo.Id, err.Error())
		return err
	}

    insertSql := "INSERT INTO JobInfo(id,run,name,description,json,created,finished,"
    insertSql += "state,pipeline,workdir,logdir,smid,retrylimit,pausedstate,secid,auxinfo,priority) "
    insertSql += " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)"

    err = db.TxExec(tx, insertSql, jobInfo.Id, jobInfo.Run, jobInfo.Name, jobInfo.Description,
		jobInfo.Json, jobInfo.Created, jobInfo.Finished, jobInfo.State, jobInfo.Pipeline,
		jobInfo.WorkDir, jobInfo.LogDir, jobInfo.SMID, jobInfo.RetryLimit, jobInfo.PausedState,
        jobInfo.SecID,jobInfo.AuxInfo, jobInfo.Priority)

	if err != nil {
        DBLogger.Errorf("Fail to insert job %s to database %s in DelHisAddJob transaction\n",
            jobInfo.Id, err.Error())
		return err
	}

	return nil
}

const MAX_JOB_COUNT int = 10240
func (db *dbService) GetJobById(jobId string) (error, []*JobDBInfo) {
    querySql := "SELECT id,run,name,description,created,finished,state,"
    querySql += "json,pipeline,workdir,logdir,smid,retrylimit,pausedstate,"
    querySql += "secid,auxinfo,priority"
    querySql += " FROM JobInfo WHERE id=$1"
    rows, err := db.Query(querySql, jobId)
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: query error %s\n", err.Error())
        return err, nil
    }
    defer rows.Close()

    jobs := make([]*JobDBInfo, 0, MAX_JOB_COUNT)
    for rows.Next() {
        job := &JobDBInfo{} 
        err = rows.Scan(&job.Id, &job.Run, &job.Name, &job.Description, &job.Created, 
            &job.Finished, &job.State, &job.Json, &job.Pipeline, &job.WorkDir,
            &job.LogDir, &job.SMID, &job.RetryLimit, &job.PausedState, &job.SecID,
            &job.AuxInfo, &job.Priority)
        if err != nil {
            return err, nil
        }
        jobs = append(jobs, job)
    }

    return nil, jobs
}

func (db *dbService) GetAllJobIds() (error, []string) {
    rows, err := db.Query("SELECT id FROM JobInfo")
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: query error %s\n", err.Error())
        return err, nil
    }
    defer rows.Close()

    ids := make([]string, 0, MAX_JOB_COUNT)
    for rows.Next() {
        var jobId string
        err = rows.Scan(&jobId)
        if err != nil {
            return err, nil
        }
        ids = append(ids, jobId)
    }

    return nil, ids
}

func (db *dbService) UpdateJob(jobInfo *JobDBInfo) error {
    updateSql := "UPDATE JobInfo SET run=$1,name=$2,description=$3,json=$4,created=$5,finished=$6,"
    updateSql += "state=$7,pipeline=$8,workdir=$9,logdir=$10,smid=$11,retrylimit=$12,pausedstate=$13,"
    updateSql += "secid=$14,auxinfo=$15,priority=$16"
    updateSql += " WHERE id=$17"

    err := db.Exec(updateSql, jobInfo.Run, jobInfo.Name, jobInfo.Description,
		jobInfo.Json, jobInfo.Created, jobInfo.Finished, jobInfo.State,
		jobInfo.Pipeline, jobInfo.WorkDir, jobInfo.LogDir, jobInfo.SMID,
		jobInfo.RetryLimit, jobInfo.PausedState, jobInfo.SecID,
        jobInfo.AuxInfo, jobInfo.Id, jobInfo.Priority)

    return err
}

func (db *dbService) UpdateJobRun(jobId string, run int) error {
    updateSql := "UPDATE JobInfo SET run=$1 WHERE id=$2"
    err := db.Exec(updateSql, run, jobId)
    return err
}

func (db *dbService) UpdateJobState(jobId string, state string, pausedstate string) error {
    updateSql := "UPDATE JobInfo SET state=$1,pausedstate=$2 WHERE id=$3"
    err := db.Exec(updateSql, state, pausedstate, jobId)
    return err
}

func (db *dbService) UpdateJobPriority(jobId string, pri int) error {
    updateSql := "UPDATE JobInfo SET priority=$1 WHERE id=$2"
    err := db.Exec(updateSql, pri, jobId)
    return err
}

func (db *dbService) DeleteJob(jobId string) error {
	deleteSql := "DELETE FROM JobInfo WHERE id=$1"
    err := db.Exec(deleteSql, jobId)

    if err != nil {
        DBLogger.Errorf("Fail to delete job id %s from database\n",
            jobId)
    } else {
        DBLogger.Infof("Succeed to delete job id %s from database\n",
            jobId)
    }

    return err
}

type JobScheduleDBInfo struct {
    Id  string
    GraphInfo string
    ExecJson string
}

func (db *dbService) AddJobScheduleInfo(jobInfo *JobScheduleDBInfo) error {
    insertSql := "INSERT INTO JobScheduleInfo(id,graphinfo,execjson) "
    insertSql += " VALUES($1,$2, $3)"   
    err := db.Exec(insertSql, jobInfo.Id, jobInfo.GraphInfo,
        jobInfo.ExecJson)
    
    if err != nil {
        DBLogger.Errorf("Fail to add job %s schedule info to database\n",
            jobInfo.Id)
    } else {
        DBLogger.Infof("Succeed to add job %s schedule info to database\n",
            jobInfo.Id)
    }

    return err
}

func (db *dbService) GetJobScheduleInfo(jobId string) (error, *JobScheduleDBInfo) {
    querySql := "SELECT id,graphinfo,execjson FROM JobScheduleInfo WHERE id=$1"
    rows, err := db.Query(querySql, jobId)
    if err != nil {
        DBLogger.Errorf("DBService: get job %s schedule info fail %s\n",
            jobId, err.Error())
        return err, nil
    }
    defer rows.Close()

    jobInfo := &JobScheduleDBInfo{}
    for rows.Next() {
        err = rows.Scan(&jobInfo.Id, &jobInfo.GraphInfo, &jobInfo.ExecJson)
        if err != nil {
            DBLogger.Errorf("DBService: get job %s schedule info fail %s\n",
                jobId, err.Error())
            return err, nil
        }
        return nil, jobInfo
    }
    
    return nil, nil
}

func (db *dbService) UpdateJobScheduleInfo(jobInfo *JobScheduleDBInfo) error {
    updateSql := "UPDATE JobScheduleInfo SET execjson=$1,graphinfo=$2 WHERE id=$3"
    err := db.Exec(updateSql, jobInfo.ExecJson, jobInfo.GraphInfo,
        jobInfo.Id)
    return err
}

func (db *dbService) DeleteJobScheduleInfo(jobId string) error {
	deleteSql := "DELETE FROM JobScheduleInfo WHERE id=$1"

    err := db.Exec(deleteSql, jobId)

    if err != nil {
        DBLogger.Errorf("Fail to delete job %s schedule info from database %s\n",
            jobId, err.Error())
    } else {
        DBLogger.Infof("Succeed to delete job %s schedule info from database\n",
            jobId)
    }

    return err
}

func (db *dbService) AddJobHistory(jobInfo *JobDBInfo, schedInfo *JobScheduleDBInfo) error {
    insertSql := "INSERT INTO JobHistory(id,run,name,description,created,finished,state,pipeline,workdir,logdir,jobJson,"
    insertSql += "smid,retrylimit,failreason,schedJson,pausedstate,secid,auxinfo,priority,graphJson) "
    insertSql += " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)"

    err := db.Exec(insertSql, jobInfo.Id, jobInfo.Run, jobInfo.Name, jobInfo.Description,  
        jobInfo.Created, jobInfo.Finished, jobInfo.State, jobInfo.Pipeline,
        jobInfo.WorkDir, jobInfo.LogDir, jobInfo.Json, jobInfo.SMID,
        jobInfo.RetryLimit, jobInfo.FailReason, schedInfo.ExecJson, jobInfo.PausedState,
        jobInfo.SecID, jobInfo.AuxInfo, jobInfo.Priority, schedInfo.GraphInfo)

    if err != nil {
        DBLogger.Errorf("Fail to add job history id %s name %s to database %s\n",
            jobInfo.Id, jobInfo.Name, err.Error())
    } else {
        DBLogger.Infof("Succeed to add job history id %s name %s to database\n",
            jobInfo.Id, jobInfo.Name)
    }

    return err
}

func (db *dbService) GetJobHistoryById(jobId string) (error, *JobDBInfo, *JobScheduleDBInfo) {
    querySql := "SELECT id,run,name,description,created,finished,state,"
    querySql += "jobJson,pipeline,workdir,logdir,smid,retrylimit,pausedstate,failreason,secid,"
    querySql += "auxinfo,priority,schedJson,graphJson"
    querySql += " FROM JobHistory WHERE id=$1"
    rows, err := db.Query(querySql, jobId)
    if err != nil {
        DBLogger.Errorf("DBService: query error %s\n", err.Error())
        return err, nil, nil
    }
    defer rows.Close()

    job := &JobDBInfo{} 
    schedInfo := &JobScheduleDBInfo{}
    for rows.Next() {
        err = rows.Scan(&job.Id, &job.Run, &job.Name, &job.Description, &job.Created, 
            &job.Finished, &job.State, &job.Json, &job.Pipeline, &job.WorkDir,
            &job.LogDir, &job.SMID, &job.RetryLimit, &job.PausedState, &job.FailReason,
            &job.SecID, &job.AuxInfo, &job.Priority, &schedInfo.ExecJson, &schedInfo.GraphInfo)
        if err != nil {
            return err, nil, nil
        }
        schedInfo.Id = job.Id
        return nil, job, schedInfo
    }

    return err, nil, nil
}

func (db *dbService) GetHistoryJobList(ctxt *SecurityContext,
    opt *JobListOpt) (error, []BioflowJobInfo) {
    querySql := "SELECT id,name,created,finished,state,description,pipeline,secid,priority"
    querySql += " FROM JobHistory"

	var err error

	i := 1

	args := make([]interface{}, 0)

	if opt.Name != "" {
		querySql += " WHERE"
		querySql += fmt.Sprintf(" name=$%d", i)

		args = append(args, opt.Name)
		i += 1
	}

	if opt.Pipeline != "" {
		if len(args) == 0 {
			querySql += " WHERE"
		} else {
			querySql += " AND"
		}
		querySql += fmt.Sprintf(" pipeline=$%d", i)
		args = append(args, opt.Pipeline)
		i += 1
	}

	if opt.State != "" {
		if len(args) == 0 {
			querySql += " WHERE"
		} else {
			querySql += " AND"
		}
		querySql += fmt.Sprintf(" state=$%d", i)
		args = append(args, strings.ToUpper(opt.State))
		i += 1
	}

	if opt.Priority != -1 {
		if len(args) == 0 {
			querySql += " WHERE"
		} else {
			querySql += " AND"
		}
		querySql += fmt.Sprintf(" priority=$%d", i)
		args = append(args, opt.Priority)
		i += 1
	}

	if opt.After != "" {
		if len(args) == 0 {
			querySql += " WHERE"
		} else {
			querySql += " AND"
		}

		if opt.Finished == "" {
			querySql += " created"
		} else {
			querySql += " finished"
		}
		querySql += fmt.Sprintf(" > $%d", i)
		args = append(args, opt.After)
		i += 1
	}

	if opt.Before != "" {
		if len(args) == 0 {
			querySql += " WHERE"
		} else {
			querySql += " AND"
		}

		if opt.Finished == "" {
			querySql += " created"
		} else {
			querySql += " finished"
		}
		args = append(args, opt.Before)
		querySql += fmt.Sprintf(" < $%d", i)
		i += 1
	}

	if opt.Count != 0 {
		querySql += fmt.Sprintf(" LIMIT $%d", i)
		args = append(args, opt.Count)
		i += 1
	}

	var rows *sql.Rows

	if len(args) == 0 {
		rows, err = db.Query(querySql)
	} else {
		rows, err = db.Query(querySql, args...)
	}

    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: query error %s\n", err.Error())
        return err, nil
    }
    defer rows.Close()

    jobInfos := make([]BioflowJobInfo, 0, MAX_JOB_COUNT)

    for rows.Next() {
        job := BioflowJobInfo{}
        err = rows.Scan(&job.JobId, &job.Name, &job.Created, &job.Finished,
            &job.State, &job.Description, &job.Pipeline, &job.Owner,
            &job.Priority)
        if err != nil {
            return err, nil
        }
        if ctxt.CheckSecID(job.Owner) {
            jobInfos = append(jobInfos, job)
        }
    }

    return nil, jobInfos
}

func (db *dbService) GetJobIdsFromHistory(id string) (error, []string) {
	querySql := "SELECT id FROM JobHistory "
	querySql += "WHERE id LIKE '%' || $1 || '%'"

    rows, err := db.Query(querySql, id)
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: query error %s\n", err.Error())
        return err, nil
    }
    defer rows.Close()

    jobIds := make([]string, 0)

    var jobId string

    for rows.Next() {
        err = rows.Scan(&jobId)
        if err != nil {
            return err, nil
        }
        jobIds = append(jobIds, jobId)
    }

    return nil, jobIds
}


func (db *dbService) DeleteJobHistory(jobId string) error {
	deleteSql := "DELETE FROM JobHistory WHERE id=$1"
    err := db.Exec(deleteSql, jobId)

    if err != nil {
        DBLogger.Errorf("Fail to delete job %s history from database %s\n",
            jobId, err.Error())
    } else {
        DBLogger.Infof("Succeed to delete job %s history from database\n",
            jobId)
    }

    return err
}

const (
    PSTATE_HEAD int = 0
    PSTATE_HIST int = 1
)

type PipelineDBInfo struct {
    Name string
    SecID string
    State int
    Parent string
    LastVersion string
    Version string
    AuxInfo string
    Json string
}

type PipelineDBKey struct {
    Name string
    SecID string
}

func (db *dbService) AddPipeline(pipelineInfo *PipelineDBInfo) error {
    insertSql := "INSERT INTO Pipelines(name,secid,state,parent,lastversion,version,auxinfo,json) "
    insertSql += " VALUES($1,$2,$3,$4,$5,$6,$7,$8)"
    
    err := db.Exec(insertSql, pipelineInfo.Name,
        pipelineInfo.SecID, pipelineInfo.State, pipelineInfo.Parent,
        pipelineInfo.LastVersion, pipelineInfo.Version, pipelineInfo.AuxInfo,
        pipelineInfo.Json)
    
    if err != nil {
        DBLogger.Errorf("Fail to add pipeline %s to database\n",
            pipelineInfo.Name)
    } else {
        DBLogger.Infof("Succeed to add pipeline %s to database\n",
            pipelineInfo.Name)
    }

    return err
}

/*Get the pipeline with latest version*/
func (db *dbService) GetPipeline(key *PipelineDBKey) (error, *PipelineDBInfo) {
    return db._GetPipeline(key, PSTATE_HEAD, "")
}

func (db *dbService) GetPipelineByVersion(key *PipelineDBKey, version string) (error, *PipelineDBInfo) {
    return db._GetPipeline(key, -1, version)
}

func (db *dbService) GetPipelineAllVersions(key *PipelineDBKey) (error, []*PipelineDBInfo) {
    querySql := "SELECT name,secid,state,parent,lastversion,version,auxinfo,json"
    querySql += " FROM Pipelines WHERE name=$1 "

    args := make([]interface{}, 0)
    args = append(args, key.Name)

    rows, err := db.Query(querySql, args...)
    if err != nil {
        db.QueryErr(err)
        DBLogger.Errorf("DBService: get pipeline %s all versions info fail %s\n",
            key.Name, err.Error())
        return err, nil
    }
    defer rows.Close()

    pipelineVersionsInfo := make([]*PipelineDBInfo, 0)
    for rows.Next() {
        pipelineInfo := &PipelineDBInfo{}
        err = rows.Scan(&pipelineInfo.Name, &pipelineInfo.SecID,
            &pipelineInfo.State, &pipelineInfo.Parent, &pipelineInfo.LastVersion,
            &pipelineInfo.Version, &pipelineInfo.AuxInfo, &pipelineInfo.Json)
        if err != nil {
            DBLogger.Errorf("DBService: get pipeline %s fail %s\n",
                key.Name, err.Error())
            return err, nil
        }
        pipelineVersionsInfo = append(pipelineVersionsInfo, pipelineInfo)
    }

    return nil, pipelineVersionsInfo
}

/*Get the pipeline with speicified version*/
func (db *dbService) _GetPipeline(key *PipelineDBKey, state int, version string) (error, *PipelineDBInfo) {
    querySql := "SELECT name,secid,state,parent,lastversion,version,auxinfo,json"
    querySql += " FROM Pipelines WHERE name=$1 "
	args := make([]interface{}, 0)
    args = append(args, key.Name)
    argIndex := 2
    if key.SecID != "" {
        querySql += fmt.Sprintf(" AND secid=$%d ", argIndex)
        argIndex ++
        args = append(args, key.SecID)
    }
    if state >= 0 {
        querySql += fmt.Sprintf(" AND state=$%d ", argIndex)
        argIndex ++
        args = append(args, state)
    }
    if version != "" {
        querySql += fmt.Sprintf(" AND version=$%d ", argIndex)
        argIndex ++
        args = append(args, version)
    }

    rows, err := db.Query(querySql, args...)
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: get pipeline %s/%s/%d info fail %s\n",
            key.Name, key.SecID, state, err.Error())
        return err, nil
    }
    defer rows.Close()

    pipelineInfo := &PipelineDBInfo{}
    for rows.Next() {
        err = rows.Scan(&pipelineInfo.Name, &pipelineInfo.SecID,
            &pipelineInfo.State, &pipelineInfo.Parent, &pipelineInfo.LastVersion,
            &pipelineInfo.Version, &pipelineInfo.AuxInfo, &pipelineInfo.Json)
        if err != nil {
            DBLogger.Errorf("DBService: get pipeline %s fail %s\n",
                key.Name, err.Error())
            return err, nil
        }
        return nil, pipelineInfo
    }
    
    return nil, nil
}

func (db *dbService) GetAllPipelineNames() (error, []PipelineDBKey) {
    querySql := "SELECT name,secid FROM Pipelines WHERE state=$1"
    rows, err := db.Query(querySql, PSTATE_HEAD)
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: get pipeline names fail %s\n",
            err.Error())
        return err, nil
    }
    defer rows.Close()

    keys := make([]PipelineDBKey, 0)
    for rows.Next() {
        key := PipelineDBKey{}
        err = rows.Scan(&key.Name, &key.SecID)
        if err != nil {
            DBLogger.Errorf("DBService: get pipeline keys fail %s\n",
                err.Error())
            return err, nil
        }
        keys = append(keys, key)
    }
    
    return nil, keys
}

/*
 * The pipeline update in database is a complicated transaction:
 * 1) Update the current row of pipeline in DB to history version by setting
 *    its state to PSTATE_HIST
 * 2) Insert a new row to represent the current version pipeline
 */
func (db *dbService) UpdatePipeline(pipelineInfo *PipelineDBInfo) error {
	var err error
	err = nil

	tx, err := db.db.Begin()
    if err != nil {
        DBLogger.Errorf("DBService: updatePipeline Begin starts a transaction err:%s",
            err.Error())
        return err
    }
	defer func() {
		if tx != nil {
			if err != nil {
				tx.Rollback()
			} else {
				tx.Commit()
			}
		}
	}()

	updateSql := "UPDATE Pipelines SET state=$1 WHERE name=$2 AND secid=$3 AND state=$4"

    err = db.TxExec(tx, updateSql, PSTATE_HIST, pipelineInfo.Name,
            pipelineInfo.SecID, PSTATE_HEAD)
	if err != nil {
        DBLogger.Errorf("Fail to set current latest version pipeline %s as history version one\n",
            pipelineInfo.Name, err.Error())
		return err
	}


    insertSql := "INSERT INTO Pipelines(name,secid,state,parent,lastversion,version,auxinfo,json) "
    insertSql += " VALUES($1,$2,$3,$4,$5,$6,$7,$8)"
    
    err = db.TxExec(tx, insertSql, pipelineInfo.Name,
        pipelineInfo.SecID, PSTATE_HEAD, pipelineInfo.Parent,
        pipelineInfo.LastVersion, pipelineInfo.Version, pipelineInfo.AuxInfo,
        pipelineInfo.Json)
    
    if err != nil {
        DBLogger.Errorf("Fail to update version %s pipeline %s to database\n",
            pipelineInfo.Version, pipelineInfo.Name)
        return err
    } else {
        DBLogger.Infof("Succeed to update pipeline %s version %s to database\n",
            pipelineInfo.Name, pipelineInfo.Version)
    }

	return nil
}

func (db *dbService) DeletePipeline(name string, secid string) error {
	deleteSql := "DELETE FROM Pipelines WHERE name=$1"
    err := db.Exec(deleteSql, name)

    if err != nil {
        DBLogger.Errorf("Fail to delete pipeline %s/%s from database %s\n",
            name, secid, err.Error())
    } else {
        DBLogger.Infof("Succeed to delete pipeline %s/%s from database\n",
            name, secid)
    }
    
    return err
}

type PipelineItemDBInfo struct {
    Name string
    SecID string
    State int
    Json string
}

type PipelineItemDBKey struct {
    Name string
    SecID string
}

func (db *dbService) AddPipelineItem(itemInfo *PipelineItemDBInfo) error {
    insertSql := "INSERT INTO PipelineItems(name,secid,state,json) "
    insertSql += " VALUES($1,$2,$3,$4)"
    err := db.Exec(insertSql, itemInfo.Name, itemInfo.SecID,
        itemInfo.State, itemInfo.Json)
    
    if err != nil {
        DBLogger.Errorf("Fail to add pipeline item %s/%s to database\n",
            itemInfo.Name, itemInfo.SecID)
    } else {
        DBLogger.Infof("Succeed to add pipeline item %s/%s to database\n",
            itemInfo.Name, itemInfo.SecID)
    }

    return err
}

func (db *dbService) GetPipelineItem(key *PipelineItemDBKey) (error, *PipelineItemDBInfo) {
    querySql := "SELECT name,secid,state,json FROM PipelineItems WHERE name=$1 AND secid=$2"
    rows, err := db.Query(querySql, key.Name, key.SecID)
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: get pipeline item %s/%s info fail %s\n",
            key.Name, key.SecID, err.Error())
        return err, nil
    }
    defer rows.Close()

    itemInfo := &PipelineItemDBInfo{}
    for rows.Next() {
        err = rows.Scan(&itemInfo.Name, &itemInfo.SecID, &itemInfo.State,
            &itemInfo.Json)
        if err != nil {
            DBLogger.Errorf("DBService: get pipeline item %s/%s fail %s\n",
                key.Name, key.SecID, err.Error())
            return err, nil
        }
        return nil, itemInfo
    }
    
    return nil, nil
}

func (db *dbService) GetAllPipelineItemNames() (error, []PipelineItemDBKey) {
    querySql := "SELECT name,secid FROM PipelineItems"
    rows, err := db.Query(querySql)
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: get pipeline names fail %s\n",
            err.Error())
        return err, nil
    }
    defer rows.Close()

    keys := make([]PipelineItemDBKey, 0)
    for rows.Next() {
        key := PipelineItemDBKey{}
        err = rows.Scan(&key.Name, &key.SecID)
        if err != nil {
            DBLogger.Errorf("DBService: get pipeline item names fail %s\n",
                err.Error())
            return err, nil
        }
        keys = append(keys, key)
    }
    
    return nil, keys
}

func (db *dbService) UpdatePipelineItem(itemInfo *PipelineItemDBInfo) error {
    updateSql := "UPDATE PipelineItems SET json=$1,state=$2 WHERE name=$3 AND secid=$4"
    err := db.Exec(updateSql, itemInfo.Json, itemInfo.State, itemInfo.Name,
        itemInfo.SecID)
    return err
}

func (db *dbService) DeletePipelineItem(name string, secid string) error {
	deleteSql := "DELETE FROM PipelineItems WHERE name=$1 AND secid=$2"
    err := db.Exec(deleteSql, name, secid)
    if err != nil {
        DBLogger.Errorf("Fail to delete pipeline item %s/%s from database %s\n",
            name, secid, err.Error())
    } else {
        DBLogger.Infof("Succeed to delete pipeline item %s/%s from database\n",
            name, secid)
    }
    
    return err
}


type UserDBInfo struct {
    ID string
    Name string
    Group string
    Credit int64
    JobQuota int64
    TaskQuota int64
    AuxInfo string
    Mail string
}

func (db *dbService) AddUserInfo(userInfo *UserDBInfo) error {
    insertSql := "INSERT INTO userinfo(id,name,groupid,credit,jobquota,taskquota,auxinfo,mail) "
    insertSql += " VALUES($1,$2,$3,$4,$5,$6,$7,$8)"
    err := db.Exec(insertSql, userInfo.ID, userInfo.Name, userInfo.Group,
        userInfo.Credit, userInfo.JobQuota, userInfo.TaskQuota, userInfo.AuxInfo, userInfo.Mail)
    if err != nil {
        DBLogger.Errorf("Fail to add user info %s/%s to database\n",
            userInfo.ID, userInfo.Name)
    } else {
        DBLogger.Infof("Succeed to add user %s/%s to database\n",
            userInfo.ID, userInfo.Name)
    }

    return err
}

func (db *dbService) GetUserInfo(id string) (error, *UserDBInfo) {
    querySql := "SELECT id,name,groupid,credit,jobquota,taskquota,auxinfo,mail FROM UserInfo WHERE id=$1"
    rows, err := db.Query(querySql, id)
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: get user %s info fail %s\n",
            id, err.Error())
        return err, nil
    }
    defer rows.Close()

    userInfo := &UserDBInfo{}
    for rows.Next() {
        err = rows.Scan(&userInfo.ID, &userInfo.Name, &userInfo.Group,
            &userInfo.Credit, &userInfo.JobQuota, &userInfo.TaskQuota,
            &userInfo.AuxInfo, &userInfo.Mail)
        if err != nil {
            DBLogger.Errorf("DBService: get user %s info fail %s\n",
                id, err.Error())
            return err, nil
        }
        return nil, userInfo
    }
    
    return nil, nil
}

func (db *dbService) GetAllUsers() (error, []UserDBInfo) {
    querySql := "SELECT id,name,groupid,credit,jobquota,taskquota,auxinfo,mail FROM UserInfo"
    rows, err := db.Query(querySql)
    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: get users info fail %s\n",
            err.Error())
        return err, nil
    }
    defer rows.Close()

    userInfoList := make([]UserDBInfo, 0)
    for rows.Next() {
        userInfo := UserDBInfo{}
        err = rows.Scan(&userInfo.ID, &userInfo.Name, &userInfo.Group,
            &userInfo.Credit, &userInfo.JobQuota, &userInfo.TaskQuota,
            &userInfo.AuxInfo, &userInfo.Mail)
        if err != nil {
            DBLogger.Errorf("DBService: get user info fail %s\n",
                err.Error())
            return err, nil
        }
        userInfoList = append(userInfoList, userInfo)
    }
    
    return nil, userInfoList
}

func (db *dbService) UpdateUser(userInfo *UserDBInfo) error {
    updateSql := "UPDATE UserInfo SET credit=$1,jobquota=$2,taskquota=$3,name=$4,groupid=$5,auxinfo=$6,mail=$7"
    updateSql += " WHERE id=$8"
    err := db.Exec(updateSql, userInfo.Credit, userInfo.JobQuota, userInfo.TaskQuota,
        userInfo.Name, userInfo.Group, userInfo.AuxInfo, userInfo.Mail, userInfo.ID)
    return err
}

func (db *dbService) DeleteUserInfo(id string) error {
	deleteSql := "DELETE FROM UserInfo WHERE id=$1"
    err := db.Exec(deleteSql, id)
    if err != nil {
        DBLogger.Errorf("Fail to delete user info %s from database %s\n",
            id, err.Error())
    } else {
        DBLogger.Infof("Succeed to delete user %s info from database\n",
            id)
    }
    
    return err
}

type AuditRscLog struct {
    JobId string
    StageId string
	Sched string
	UserId string
	Duration float64
	Cpu float64
	Mem float64
}

func (db *dbService) AddAuditRscLog(aLog *AuditRscLog) error {
    insertSql := "INSERT INTO AuditRscLogs(jobid,stageid,schedtime,userid,"
	insertSql += "duration,cpu,mem) VALUES($1,$2,$3,$4,$5,$6,$7)"

    err := db.Exec(insertSql, aLog.JobId, aLog.StageId, aLog.Sched, aLog.UserId,
	    aLog.Duration, aLog.Cpu, aLog.Mem)

    if err != nil {
        DBLogger.Errorf("Fail to add audit log job %s stage %s of user %s.\n",
            aLog.JobId, aLog.StageId, aLog.UserId)
    } else {
        DBLogger.Infof("Succeed to add audit log job %s stage %s of user %s.\n",
            aLog.JobId, aLog.StageId, aLog.UserId)
    }

    return err
}

/*Ratio entry means that a user's information entry is greater than the value of this entry divided by 60*/
var RatioEntry uint64 = 3000
var Ratio int = 60

func (db *dbService) _GetUsersFromAuditRscLog() (error, []string) {
    querySql := "SELECT userid from AuditRscLogs GROUP by userid";
    var rows *sql.Rows
    var err error

    rows, err = db.Query(querySql)
    if err != nil {
        db.QueryErr(err)
        DBLogger.Errorf("DBService: query error %s\n", err.Error())
        return err, nil
    }
    defer rows.Close()

    users := make([]string, 0)
    for rows.Next() {
        user := ""
        err = rows.Scan(&user)
        if err != nil {
            DBLogger.Errorf("DBService: get users fail %s\n",
                err.Error())
            return err, nil
        }
        users = append(users, user)
    }
    return nil, users
}

func (db *dbService) GetUserRscStat(user string, userRecStat *UserRscStats) error {
    querySql := "SELECT count(jobid), count(stageid), sum(cpu*duration/60), sum(mem*duration/60)" +
        " FROM AuditRscLogs WHERE userid=$1"
    rows, err := db.Query(querySql, user)
    if err != nil {
        db.QueryErr(err)
        DBLogger.Errorf("DBService: query error %s\n", err.Error())
        return err
    }
    for rows.Next() {
        userStat := UserRscStats{}
        err = rows.Scan(&userStat.JobCnt, &userStat.StageCnt,
            &userStat.CpuMinutes, &userStat.MemMinutes)
        if err != nil {
            DBLogger.Errorf("DBService: get user stats fail %s\n",
                err.Error())
            return err
        }
        userRecStat.MemMinutes = userStat.MemMinutes*(float64(Ratio))
        userRecStat.CpuMinutes = userStat.CpuMinutes*(float64(Ratio))
        userRecStat.JobCnt = userStat.JobCnt
        userRecStat.StageCnt = userStat.StageCnt
        userRecStat.ID = user
        return nil
    }
    return nil
}

func (db *dbService) GetUserRscStats(opt *UserRscStatsOpt) (error, []UserRscStats) {
    querySql := "SELECT userid, count(jobid), count(stageid), sum(cpu*duration), sum(mem*duration) FROM AuditRscLogs"

	i := 1
	args := make([]interface{}, 0)

	if opt. UserId!= "" {
		querySql += fmt.Sprintf(" userid=$%d", i)
		args = append(args, opt.UserId)
		i += 1
	}

	if opt.After != "" {
		if len(args) == 0 {
			querySql += " WHERE"
		} else {
			querySql += " AND"
		}

		querySql += " schedtime"
		querySql += fmt.Sprintf(" > $%d", i)
		args = append(args, opt.After)
		i += 1
	}

	if opt.Before != "" {
		if len(args) == 0 {
			querySql += " WHERE"
		} else {
			querySql += " AND"
		}

		querySql += " schedtime"

		args = append(args, opt.Before)
		querySql += fmt.Sprintf(" < $%d", i)
		i += 1
	}

    querySql += "  GROUP by userid;"

	var rows *sql.Rows
	var err error

	if len(args) == 0 {
		rows, err = db.Query(querySql)
	} else {
		rows, err = db.Query(querySql, args...)
	}

    if err != nil {
		db.QueryErr(err)
        DBLogger.Errorf("DBService: query error %s\n", err.Error())
        return err, nil
    }
    defer rows.Close()

    userStatsList := make([]UserRscStats, 0)
    for rows.Next() {
        userStats := UserRscStats{}
        err = rows.Scan(&userStats.ID, &userStats.JobCnt, &userStats.StageCnt,
			&userStats.CpuMinutes, &userStats.MemMinutes)
        if err != nil {
            DBLogger.Errorf("DBService: get user stats fail %s\n",
                err.Error())
            return err, nil
        }
        userStatsList = append(userStatsList, userStats)
    }

    for i := 0; i < len(userStatsList); i++ {
        if userStatsList[i].JobCnt >= RatioEntry {
            userStat := UserRscStats{}
            err := db.GetUserRscStat(userStatsList[i].ID, &userStat)
            if err != nil {
                DBLogger.Errorf("DBService: get user %s stats fail %s\n",
                    userStatsList[i].ID, err.Error())
                return err, nil
            }
            userStatsList[i] = userStat
        }
    }
    return nil, userStatsList
}

func (db *dbService) DeleteAuditRscLogs(opt *UserRscStatsOpt) error {
	deleteSql := "DELETE FROM AuditRscLogs"
	i := 1
	args := make([]interface{}, 0)

	if opt. UserId!= "" {
		deleteSql += " WHERE"
		deleteSql += fmt.Sprintf(" userid=$%d", i)
		args = append(args, opt.UserId)
		i += 1
	}

	if opt.After != "" {
		if len(args) == 0 {
			deleteSql += " WHERE"
		} else {
			deleteSql += " AND"
		}

		deleteSql += " schedtime"
		deleteSql += fmt.Sprintf(" > $%d", i)
		args = append(args, opt.After)
		i += 1
	}

	if opt.Before != "" {
		if len(args) == 0 {
			deleteSql += " WHERE"
		} else {
			deleteSql += " AND"
		}

		deleteSql += " schedtime"

		args = append(args, opt.Before)
		deleteSql += fmt.Sprintf(" < $%d", i)
		i += 1
	}

	var err error

	if len(args) == 0 {
		err = db.Exec(deleteSql)
	} else {
		err = db.Exec(deleteSql, args...)
	}

    if err != nil {
        DBLogger.Errorf("Fail to delete user stats from database %s\n",
            err.Error())
    } else {
        DBLogger.Infof("Succeed to delete user stats from database\n")
    }
    
    return err
}
