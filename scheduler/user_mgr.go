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
package scheduler

import (
    "sync"
    "sync/atomic"
    "errors"
    "time"
    "strings"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
    . "github.com/xtao/bioflow/confbase"
    . "github.com/xtao/bioflow/dbservice"
    . "github.com/xtao/bioflow/scheduler/common"
    "github.com/xtao/bioflow/eventbus"
    ldapclient "github.com/xtao/xldap/client"
    "fmt"
)

const (
    DEF_MAX_TASK_NUM int64 = 500
    DEF_MAX_JOB_NUM int64 = 100
    COMPLETE string = "COMPLETE"
    SCHEDULED string = "SCHEDULED"
    ERROR string = "ERROR"
    FAIL string = "FAIL"
    LOST string = "LOST"
    CANCELED string = "CANCELD"
    PAUSED string = "PAUSED"
    RESUMED string = "RESUMED"
)

/*
 * Bioflow don't create and mantain users, it just access data
 * with right user priviledge. All users are imported from ETCD,
 * which is configured by powerCLI.
 * User information are maintained three ways:
 * 1) user account information (e.g, user id, name, group) are imported
 *    from external sources like ETCD. Bioflow don't persist them.
 * 2) user access mask are submitted along with each job.
 * 3) user credit, quota are mantained by Bioflow and persist into
 *    database.
 */
type UserInfo struct {
    account *UserAccountInfo
    jobNum int64
    taskNum int64
    pausedJobNum int64
    runningJobNum int64
    failJobNum int64
    canceledJobNum int64
    completeJobNum int64
    lostTaskNum int64

    credit int64
    jobQuota int64
    taskQuota int64
    mail string
    subscriber *eventbus.Subscriber
    groups map[string] bool
}

func NewUserInfo(account *UserAccountInfo) *UserInfo {
    return &UserInfo{
            account: account,
            jobNum: 0,
            taskNum: 0,
            pausedJobNum: 0,
            failJobNum: 0,
            canceledJobNum: 0,
            completeJobNum: 0,
            lostTaskNum: 0,
            runningJobNum: 0,
            groups: make(map[string]bool),
    }
}

func (user *UserInfo) ID() string {
    if user.account != nil {
        return user.account.Username
    }

    return "anonymous"
}

func (user *UserInfo) Name() string {
    if user.account != nil {
        return user.account.Username
    }

    return ""
}

func (user *UserInfo) Group() string {
    if user.account != nil {
        return user.account.Groupname
    }

    return ""
}

func (user *UserInfo) RecoverFromDB(info *UserDBInfo) {
    user.credit = info.Credit
    user.jobQuota = info.JobQuota
    user.taskQuota = info.TaskQuota
    user.mail = info.Mail
    if user.mail != "" {
        user.subscriber = eventbus.NewSubscriber()
        user.subscriber.Unsubscribe(user.ID())
        handler := NewMailNoticeHandler(user.mail, "Job Status Update.", JobStatusToMsg)
        user.subscriber.Subscribe(user.ID(), handler.Handle, true)
        SchedulerLogger.Infof("User %s subscribe job event", user.ID())
    }
}

func (user *UserInfo) CreateUserDBInfo() *UserDBInfo {
    return &UserDBInfo{
        ID: user.ID(),
        Name: user.Name(),
        Group: user.Group(),
        Credit: user.credit,
        JobQuota: user.jobQuota,
        TaskQuota: user.taskQuota,
        Mail: user.mail,
    }
}

func (user *UserInfo) AccountInfo() *UserAccountInfo {
    return user.account
}

type userMgr struct {
    lock sync.RWMutex

    /*track all valid users*/
    users map[string]*UserInfo

    /*security mode*/
    secMode int

    /*quota configuration*/
    maxTaskNum int64
    maxJobNum int64

    ldapServer string
    useLDAP bool
    etcdEndPoints []string
    ldapDB *ldapclient.LdapDB
}

var globalUserMgr *userMgr = nil

func NewUserMgr() *userMgr {
    userMgr := &userMgr{
        users: make(map[string]*UserInfo),
        secMode: SECURITY_MODE_ISOLATION,
        maxTaskNum: DEF_MAX_TASK_NUM,
        maxJobNum: DEF_MAX_JOB_NUM,
    }

    subscriber := eventbus.NewSubscriber()
    subscriber.Subscribe(JOBEVENTID, userMgr._HandleJobEvent, true)
    return userMgr
}

func GetUserMgr() *userMgr {
    if globalUserMgr == nil {
        globalUserMgr = NewUserMgr()
    }

    return globalUserMgr
}

func (userMgr *userMgr) UseLDAP() bool {
    return userMgr.useLDAP
}

func (userMgr *userMgr) Configure(config *SecurityConfig, endpoints []string) error {
    switch config.SecMode {
        case SEC_MODE_ISOLATION:
            SchedulerLogger.Infof("Set user manager to isolation mode\n")
            userMgr.SetSecurityMode(SECURITY_MODE_ISOLATION)
        case SEC_MODE_STRICT:
            SchedulerLogger.Infof("Set user manager to strict mode\n")
            userMgr.SetSecurityMode(SECURITY_MODE_STRICT)
        default:
            SchedulerLogger.Warnf("Unknown security mode: %s, ignore it\n",
                config.SecMode)
    }

    if config.MaxTaskNum <= 0 {
        userMgr.maxTaskNum = DEF_MAX_TASK_NUM
    } else {
        userMgr.maxTaskNum = config.MaxTaskNum
    }
    if config.MaxJobNum <= 0 {
        userMgr.maxJobNum = DEF_MAX_JOB_NUM
    } else {
        userMgr.maxJobNum = config.MaxJobNum
    }

    userMgr.ldapServer = config.LDAPServer
    userMgr.useLDAP = config.UseLDAP
    if userMgr.useLDAP {
        userMgr.ldapDB = ldapclient.NewLdapDB(strings.Split(config.LDAPServer, ","))
    }

    if endpoints != nil {
        userMgr.etcdEndPoints = endpoints
    }

    return nil
}

func (userMgr *userMgr) MergeUserInfo(newInfo *UserInfo, oldInfo *UserInfo) {
    if oldInfo != nil && newInfo != nil {
        newInfo.credit = oldInfo.credit
        newInfo.jobQuota = oldInfo.jobQuota
        newInfo.taskQuota = oldInfo.taskQuota
        newInfo.jobNum = oldInfo.jobNum
        newInfo.taskNum = oldInfo.taskNum
        newInfo.pausedJobNum = oldInfo.pausedJobNum
        newInfo.runningJobNum = oldInfo.runningJobNum
        newInfo.failJobNum = oldInfo.failJobNum
        newInfo.canceledJobNum = oldInfo.canceledJobNum
        newInfo.completeJobNum = oldInfo.completeJobNum
        newInfo.lostTaskNum = oldInfo.lostTaskNum
        newInfo.mail = oldInfo.mail
    }
}

/*update user info from ETCD to user manager*/
func (userMgr *userMgr) UpdateUsers(users []UserAccountInfo, groups map[string][]string) error {
    newUsers := make(map[string]*UserInfo)
    for i := 0; i < len(users); i ++ {
        userInfo := NewUserInfo(&users[i])
        userInfo.jobQuota = userMgr.maxJobNum
        userInfo.taskQuota = userMgr.maxTaskNum
        name := users[i].Username
        gid := users[i].Gid
        for groupId, usersInGroup := range groups {
            if groupId == gid {
                userInfo.groups[groupId] = true
            }
            for _, user := range usersInGroup {
                if name == user {
                    userInfo.groups[groupId] = true
                }
            }
        }
        /*
         * If the user already exists, so need update the credit info
         * from current user info. because that ETCD only keeps the
         * user account information, don't have credit and quota. The
         * credit and quota are stored in databases.
         */
        userMgr.lock.RLock()
        if oldInfo, ok := userMgr.users[name]; ok {
            userMgr.MergeUserInfo(userInfo,
                oldInfo)
        }
        userMgr.lock.RUnlock()
        newUsers[users[i].Username] = userInfo
    }

    userMgr.lock.Lock()
    userMgr.users = newUsers
    userMgr.lock.Unlock()

    return nil
}

/*recover user credit and quota from database*/
func (userMgr *userMgr) RecoverUserCreditQuotaFromDB() error {
    dbService := GetDBService()

    err, dbUsers := dbService.GetAllUsers()
    if err != nil {
        SchedulerLogger.Errorf("Fail to load user credit quota from db: %s\n",
            err.Error())
        return err
    }

    for i := 0; i < len(dbUsers); i ++ {
        userDBInfo := dbUsers[i]
        userInfo := userMgr.GetUserInfo(userDBInfo.ID)
        if userInfo != nil {
           userInfo.RecoverFromDB(&userDBInfo) 
        }
    }

    return nil
}

func (userMgr *userMgr) LoadExternalUsers(retryCount int) error {
    if userMgr.useLDAP {
        return userMgr.LoadExternalUsersFromLDAP(userMgr.ldapServer, retryCount)
    } else {
        return userMgr.LoadExternalUsersFromETCD(userMgr.etcdEndPoints, retryCount)
    }
}

/*load users and groups from ETCD*/
func (userMgr *userMgr) LoadExternalUsersFromETCD(endpoints []string,
    retryCount int) error {
    userDB := NewUserDB(endpoints)
    var err error = nil
    var userMap map[string]UserEntry = nil
    var groupMap map[string]GroupEntry = nil
    for retry := 0; retry <= retryCount; retry ++ {
        err, userMap = userDB.GetAllUsers()
        if err != nil {
            SchedulerLogger.Errorf("Fail get users from %v: %s\n",
                endpoints, err.Error())
            time.Sleep(time.Duration(10) * time.Second)
        } else {
            err, groupMap = userDB.GetAllGroups()
            if err != nil {
                SchedulerLogger.Errorf("Fail get groups from %v: %s\n",
                    endpoints, err.Error())
                time.Sleep(time.Duration(10) * time.Second)
            } else {
                break
            }
        }
    }
    if err != nil {
        SchedulerLogger.Errorf("Fail to load users or groups from external ETCD %v\n",
            endpoints)
        return err
    } else {
        return userMgr._UpdateUserGroupMap(userMap, groupMap)
    }
}

/*load users and groups from LDAP servers*/
func (userMgr *userMgr) LoadExternalUsersFromLDAP(ldapServer string,
    retryCount int) error {
    ldapDB := userMgr.ldapDB
    if ldapDB == nil {
        ldapDB = ldapclient.NewLdapDB(strings.Split(ldapServer, ","))
        if ldapDB == nil {
            return errors.New("Fail to init the ldap client")
        }
    }
    var err error = nil
    var userMap map[string]ldapclient.UserEntry = nil
    var groupMap map[string]ldapclient.GroupEntry = nil
    for retry := 0; retry <= retryCount; retry ++ {
        err, userMap = ldapDB.GetAllUsers()
        if err != nil {
            SchedulerLogger.Errorf("Fail get users from %v: %s\n",
                ldapServer, err.Error())
            time.Sleep(time.Duration(10) * time.Second)
        } else {
            err, groupMap = ldapDB.GetAllGroups()
            if err != nil {
                SchedulerLogger.Errorf("Fail get groups from %v: %s\n",
                    ldapServer, err.Error())
                time.Sleep(time.Duration(10) * time.Second)
            } else {
                break
            }
        }
    }

    if err != nil {
        SchedulerLogger.Errorf("Fail to load users or groups from external LDAP server %v\n",
            ldapServer)
        return err
    } else {
        /*transfer from ldap UserEntry and GroupEntry data strucutres to ourselves*/
        lUserMap := make(map[string]UserEntry)
        lGroupMap := make(map[string]GroupEntry)
        for key, entry := range userMap {
            lUserMap[key] = UserEntry{
                Pass: entry.Pass,
                Uid: entry.Uid,
                Gid: entry.Gid,
                Gecos: entry.Gecos,
                Home: entry.Home,
                Shell: entry.Shell,
            }
        }
        for key, entry := range groupMap {
            lGroupMap[key] = GroupEntry {
                Pass: entry.Pass,
                Gid: entry.Gid,
                Users: entry.Users,
            }
        }
        return userMgr._UpdateUserGroupMap(lUserMap, lGroupMap)
    }
}

func (userMgr *userMgr) _UpdateUserGroupMap(userMap map[string]UserEntry,
    groupMap map[string]GroupEntry) error {
    if userMap != nil && groupMap != nil{
        accounts := ExConfigUserEntryToAccount(userMap)
        groups := ExConfigGroupEntryToGroup(groupMap)
        return userMgr.UpdateUsers(accounts, groups)
    } else {
        SchedulerLogger.Error("The user or group map is nil\n")
        return errors.New("Nil user or group map")
    }
}

func (userMgr *userMgr) SetSecurityMode(mode int) error {
    userMgr.secMode = mode

    return nil
}

func (userMgr *userMgr) IsIsolationMode() bool {
    return userMgr.secMode == SECURITY_MODE_ISOLATION
}

func (userMgr *userMgr) IsStrictMode() bool {
    return userMgr.secMode == SECURITY_MODE_STRICT
}

func (userMgr *userMgr) _GetUserInfo(id string) *UserInfo {
    if userInfo, ok := userMgr.users[id]; ok {
        return userInfo
    }

    return nil
}

func (userMgr *userMgr) GetUserInfo(id string) *UserInfo {
    userMgr.lock.RLock()
    defer userMgr.lock.RUnlock()

    return userMgr._GetUserInfo(id)
}

func (userMgr *userMgr) GetUIDByName(name string) (string, string) {
    userMgr.lock.RLock()
    defer userMgr.lock.RUnlock()

    if userInfo := userMgr._GetUserInfo(name); userInfo != nil && userInfo.account != nil {
        return userInfo.account.Uid, userInfo.account.Gid
    } else {
        return "", ""
    }
}

func (userMgr *userMgr) ValidateUserCtxt(ctxt *SecurityContext) (error, bool) {
    /*Validate whether the user exist or not*/
    userInfo := userMgr.GetUserInfo(ctxt.ID())
    if userInfo == nil && userMgr.IsStrictMode() {
        return errors.New("Invalid user " + ctxt.ID()),
            false
    }

    return nil, true
}

func (userMgr *userMgr) AccountTasks(ctxt *SecurityContext, num int) {
    userInfo := userMgr.GetUserInfo(ctxt.ID())
    if userInfo != nil {
        atomic.AddInt64(&userInfo.taskNum, int64(num))
    }
}

func (userMgr *userMgr) AllowScheduleNewTask(ctxt *SecurityContext) bool {
    userInfo := userMgr.GetUserInfo(ctxt.ID())
    if userInfo != nil {
        if userInfo.taskNum >= userMgr.maxTaskNum {
            return false
        }

        return true
    }

    return true
}

func (userMgr *userMgr) GetUserCredit(ctxt *SecurityContext, user string) (error, int64) {
    userInfo := userMgr.GetUserInfo(user)
    if userInfo == nil {
        return errors.New("The user " + user + " not exist"),
            0
    }
    if !ctxt.CheckSecID(user) {
        return errors.New("User " + ctxt.ID() + " has no privilege "),
            0
    }

    return nil, userInfo.credit
}

func (userMgr *userMgr) GetBioflowUserInfo(ctxt *SecurityContext,
    user string) (error, *BioflowUserInfo) {
    userInfo := userMgr.GetUserInfo(user)
    if userInfo == nil {
        return errors.New("The user " + user + " not exist"),
            nil
    }

    if !ctxt.CheckSecID(user) {
        return errors.New("User " + ctxt.ID() + " has no privilege "),
            nil
    }

    bioflowUserInfo := &BioflowUserInfo {
        Username: userInfo.account.Username,
        Uid: userInfo.account.Uid,
        Gid: userInfo.account.Gid,
        Groupname: userInfo.account.Groupname,
        JobNum: userInfo.jobNum,
        TaskNum: userInfo.taskNum,
        PausedJobNum: userInfo.pausedJobNum,
        CompletedJobNum: userInfo.completeJobNum,
        CanceledJobNum: userInfo.canceledJobNum,
        FailJobNum: userInfo.failJobNum,
        LostTaskNum: userInfo.lostTaskNum,
        RunningJobNum: userInfo.runningJobNum,
        Credit: userInfo.credit,
        JobQuota: userInfo.jobQuota,
        TaskQuota: userInfo.taskQuota,
        Groups: userInfo.groups,
        Mail: userInfo.mail,
    }

    return nil, bioflowUserInfo
}

func (userMgr *userMgr) ListBioflowUsers(ctxt *SecurityContext) (error, []BioflowUserInfo) {
    if !ctxt.IsPrivileged() {
        return errors.New("User " + ctxt.ID() + " has no privilege "),
            nil
    }
    userList := make([]BioflowUserInfo, 0)

    userMgr.lock.RLock()
    defer userMgr.lock.RUnlock()
    for _, userInfo := range userMgr.users {
        bioflowUserInfo := BioflowUserInfo {
            Username: userInfo.account.Username,
            Uid: userInfo.account.Uid,
            Gid: userInfo.account.Gid,
            Groupname: userInfo.account.Groupname,
            JobNum: userInfo.jobNum,
            TaskNum: userInfo.taskNum,
            PausedJobNum: userInfo.pausedJobNum,
            CompletedJobNum: userInfo.completeJobNum,
            CanceledJobNum: userInfo.canceledJobNum,
            FailJobNum: userInfo.failJobNum,
            LostTaskNum: userInfo.lostTaskNum,
            RunningJobNum: userInfo.runningJobNum,
            Credit: userInfo.credit,
            JobQuota: userInfo.jobQuota,
            TaskQuota: userInfo.taskQuota,
            Groups: userInfo.groups,
            Mail: userInfo.mail,
        }
        userList = append(userList, bioflowUserInfo)
    }

    return nil, userList
}

func (userMgr *userMgr) RequestJobQuota(ctxt *SecurityContext, force bool) bool{
    userMgr.lock.Lock()
    defer userMgr.lock.Unlock()

    userInfo := userMgr._GetUserInfo(ctxt.ID())
    if userInfo == nil {
        return true
    } else {
        if userInfo.jobNum < userInfo.jobQuota || force {
            userInfo.jobNum ++
            return true
        } else {
            return false
        }
    }
}

func (userMgr *userMgr) ReleaseJobQuota(ctxt *SecurityContext, num int) {
    userMgr.lock.Lock()
    defer userMgr.lock.Unlock()

    userInfo := userMgr._GetUserInfo(ctxt.ID())
    if userInfo != nil {
        userInfo.jobNum -= int64(num)
    }
}

func (userMgr *userMgr) _HandleJobEvent(a interface{}) {
    if jobinfo, ok := a.(*JobInfo);ok {
        job := jobinfo.job
        state := jobinfo.state
        event := jobinfo.event
        userMgr.HandleJobScheduleEvent(job, state, event)
    }
}

func (userMgr *userMgr)HandleJobScheduleEvent(job Job, state int, event int) {
    ctxt := job.GetSecurityContext()
    if ctxt == nil {
        return
    }
    userInfo := userMgr.GetUserInfo(ctxt.ID())
    if userInfo == nil {
        return
    }
    var status string
    switch event {
        case JOB_EVENT_COMPLETE:
            SchedulerLogger.Infof("User %s job %s completed\n",
                userInfo.ID(), job.GetID())
            atomic.AddInt64(&userInfo.completeJobNum, 1)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&userInfo.runningJobNum, -1)
            }
            status = COMPLETE
        case JOB_EVENT_SCHEDULED:
            SchedulerLogger.Warnf("User %s job %s scheduled\n",
                userInfo.ID(), job.GetID())
            atomic.AddInt64(&userInfo.runningJobNum, 1)
            status = SCHEDULED
        case JOB_EVENT_ERROR:
            SchedulerLogger.Warnf("User %s job %s schedule error\n",
                userInfo.ID(), job.GetID())
            atomic.AddInt64(&userInfo.failJobNum, 1)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&userInfo.runningJobNum, -1)
            }
            status = ERROR
        case JOB_EVENT_FAIL:
            SchedulerLogger.Warnf("User %s job %s schedule fail\n",
                userInfo.ID(), job.GetID())
            atomic.AddInt64(&userInfo.failJobNum, 1)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&userInfo.runningJobNum, -1)
            }
            status = FAIL
        case JOB_EVENT_LOST:
            SchedulerLogger.Infof("User %s job %s task lost\n",
                userInfo.ID(), job.GetID())
            atomic.AddInt64(&userInfo.lostTaskNum, 1)
            status = LOST
        case JOB_EVENT_CANCELED:
            SchedulerLogger.Infof("User %s job %s canceled\n",
                userInfo.ID(), job.GetID())
            atomic.AddInt64(&userInfo.canceledJobNum, 1)
            if JobStateIsRunning(state) {
                atomic.AddInt64(&userInfo.runningJobNum, -1)
            } else if JobStateIsPaused(state) {
                atomic.AddInt64(&userInfo.pausedJobNum, -1)
            }
            status = CANCELED
        case JOB_EVENT_PAUSED:
            if JobStateIsRunning(state) {
                atomic.AddInt64(&userInfo.runningJobNum, -1)
            }
            atomic.AddInt64(&userInfo.pausedJobNum, 1)
            status = PAUSED
        case JOB_EVENT_RESUMED:
            if JobStateIsRunning(job.State()) {
                atomic.AddInt64(&userInfo.runningJobNum, 1)
            }
            atomic.AddInt64(&userInfo.pausedJobNum, -1)
            status = RESUMED
        default:
            SchedulerLogger.Debugf("Ignore the event %d for job %s user %s\n",
                event, job.GetID(), userInfo.ID())
    }
    if status != ""{
        msg := fmt.Sprintf("Job %s gets new status: %s.", job.GetID(), status)
        event := NewNoticeEvent(userInfo.ID(), msg)
        eventbus.Publish(event)
        SchedulerLogger.Infof("Publish job event of user %s", userInfo.ID())
    }
}

func (userMgr *userMgr) UpdateUserCreditQuota(ctxt *SecurityContext, userConfig *BioflowUserConfig) error{
    if !ctxt.IsPrivileged() {
        return errors.New("User " + ctxt.ID() + " has no privilege")
    }

    userMgr.lock.Lock()
    defer userMgr.lock.Unlock()

    userInfo := userMgr._GetUserInfo(userConfig.ID)
    if userInfo == nil {
        return errors.New("User not exist")
    } else {
        if userConfig.Credit >= 0 {
            userInfo.credit = userConfig.Credit
        }
        if userConfig.JobQuota >= 0 {
            userInfo.jobQuota = userConfig.JobQuota
        }
        if userConfig.TaskQuota >= 0 {
            userInfo.taskQuota = userConfig.TaskQuota
        }
        if userConfig.Mail != "" {
            userInfo.mail = userConfig.Mail
            if userInfo.subscriber == nil {
                userInfo.subscriber = eventbus.NewSubscriber()
            }
            userInfo.subscriber.Unsubscribe(userConfig.ID)
            handler := NewMailNoticeHandler(userConfig.Mail, "Job Status Update.", JobStatusToMsg)
            userInfo.subscriber.Subscribe(userConfig.ID, handler.Handle, true)
            SchedulerLogger.Infof("User %s subscribe job event", userInfo.ID())
        }

        dbInfo := userInfo.CreateUserDBInfo()
        dbService := GetDBService()
        /*check whether the user config exist*/
        err, oldInfo := dbService.GetUserInfo(userConfig.ID)
        if err != nil {
            SchedulerLogger.Errorf("Fail to access database: %s\n",
                err.Error())
            return err
        }
        if oldInfo != nil {
            err = dbService.UpdateUser(dbInfo)
            if err != nil {
                SchedulerLogger.Errorf("Fail to update user %s credit and quota: %s\n",
                    userConfig.ID, err.Error())
                return err
            }
        } else {
            err = dbService.AddUserInfo(dbInfo)
            if err != nil {
                SchedulerLogger.Errorf("Fail to add user %s credit and quota: %s\n",
                    userConfig.ID, err.Error())
                return err
            }
        }
    }

    return nil
}

func (userMgr *userMgr) Recover() error {
    err := userMgr.RecoverUserCreditQuotaFromDB()
    if err != nil {
        SchedulerLogger.Errorf("Fail to recover user credit from db: %s\n",
            err.Error())
    } else {
        SchedulerLogger.Infof("Successfully recover user credit quota\n")
    }

    return err
}

func (userMgr *userMgr) AuditResourceUsage(jobId string, stageId string,
	schTime time.Time, userId string, duration float64, cpu float64,
	mem float64) error{

    /* 
     * append job stage execution resource accounting into DB
     */
    auditRscLog := &AuditRscLog {
		JobId : jobId,
		StageId: stageId,
		Sched: schTime.Format(BIOFLOW_TIME_LAYOUT),
		UserId: userId,
		Duration: duration,
		Cpu: cpu,
		Mem: mem,
    }

    err := GetDBService().AddAuditRscLog(auditRscLog)
    if err != nil {
        SchedulerLogger.Errorf("Fail to update database user %s rsc audit %s\n",
            userId, err.Error())
        return err
    }

    return nil
}

func (userMgr *userMgr) GetRscStats(ctxt *SecurityContext, opt *UserRscStatsOpt) (error, []UserRscStats) {
    if !ctxt.IsPrivileged() {
		if opt.UserId != ctxt.ID() {
			return errors.New("User " + ctxt.ID() + " has no privilege "),
              nil
		}
    }

	return GetDBService().GetUserRscStats(opt)
}

func (userMgr *userMgr) ResetRscStats(ctxt *SecurityContext, opt *UserRscStatsOpt) error {
	if !ctxt.IsPrivileged() {
		return errors.New("User" + ctxt.ID() + "has no privilege to do the operation.")
	}
	return GetDBService().DeleteAuditRscLogs(opt)
}

func (userMgr *userMgr) CheckGroupPrivilege(ctxt *SecurityContext, pipelineOwnerName string, curUserName string) bool {
    if ctxt.IsPrivileged() {
        return true
    }

    pipelineOwnerUserInfo := userMgr.GetUserInfo(pipelineOwnerName)
    curUserInfo := userMgr.GetUserInfo(curUserName)

    if pipelineOwnerUserInfo == nil || curUserInfo == nil {
        return false
    }

    return ctxt.CheckGroup(curUserInfo.groups, pipelineOwnerUserInfo.groups)
}
