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
package main

import (
    "fmt"
    "strings"
    "gopkg.in/alecthomas/kingpin.v2"
    . "github.com/xtao/bioflow/client"
    . "github.com/xtao/bioflow/common"
)

var (

    /*
     * adminstrator command
     */
    auth = kingpin.Command("auth", "Authentication for privileged user.")

    auth_pass = auth.Command("passwd", "Change password of privileged user.")
    aepoints = auth_pass.Flag("endpoints", "ETCD endpoints in format svr1,svr2,svr3").
        Short('e').
        String()

    ag_old = auth_pass.Flag("old", "Last password for pivileged user.").
        Short('o').
        Required().
        String()

    ag_new = auth_pass.Flag("new", "New password for privileged user.").
        Short('n').
        Required().
        String()

    auth_kgen = auth.Command("keygen", "Generate API-Key and Security-Key for non-privileged account.")
    ak_nu = auth_kgen.Arg("username", "Account name").
        String()

    auth_kget= auth.Command("keyget", "Get existing API-Key and Security-Key for non-privileged account.")
    ak_tu = auth_kget.Arg("username", "Account name").
        String()


    /*
     * config commands
     */
    config = kingpin.Command("config", "Configuration managment")
    cepoints = config.Flag("endpoints", "ETCD endpoints in format svr1,svr2,svr3").
        Short('e').
        String()

    conf_init = config.Command("init", "Initialize the bioflow configuration")
    ci_file = conf_init.Arg("file", "configure file").
        Required().
        String()

    conf_update_cluster = config.Command("update-cluster", "Update the bioflow cluster configuration")
    cuc_file = conf_update_cluster.Arg("file", "configure file").
        Required().
        String()

    conf_dump = config.Command("dump", "Dump bioflow configuration")

    conf_map = config.Command("mapvol", "Map volume for scheduler or server")
    cm_type = conf_map.Flag("scheduler", "map volume on scheduler, unset means map volume on servers").
        Short('k').
        Bool()
    cm_map = conf_map.Arg("volmap", "Volume map in format vol@cluster:mnt or vol:mnt").
        Required().
        String()
    run_path = config.Command("jobrootpath","Set job root path for running job resotred on OS.")
    cmd_path = run_path.Arg("path", "Path in format vol@cluster:path/to/dir").
        Required().
        String()
    un_run_path = config.Command("unjobrootpath","Delete job root path")
    conf_unmap = config.Command("unmapvol", "Unmap volume for scheduler or server")
    cu_type = conf_unmap.Flag("scheduler", "unmap volume on scheduler, unset the option, unmap volume on servers").
        Short('k').
        Bool()
    cu_map = conf_unmap.Arg("volmap", "Volume map in format vol@cluster:mnt or vol:mnt").
        Required().
        String()

    conf_mailaccount = config.Command("mailaccount", "Set a mail account to notify events to users")
    mail_user = conf_mailaccount.Flag("user", "user of mail account").
        Required().
        Short('u').
        String()
    mail_pass = conf_mailaccount.Flag("password", "password of mail account").
        Required().
        Short('p').
        String()
    mail_host = conf_mailaccount.Flag("host", "password of mail account").
        Required().
        Short('s').
        String()

    conf_filedownload = config.Command("filedownload", "Set resource of files download work")
    t_cpu = conf_filedownload.Flag("cpu", "cpu").
        Required().
        Short('u').
        Float()
    t_mem = conf_filedownload.Flag("memory", "memory").
        Required().
        Short('m').
        Float()

    conf_frontend = config.Command("frontend", "Add or delete a front-end that receive information notice.")
    frontend_url = conf_frontend.Flag("url","url of api").
        Required().
        Short('u').
        String()
    frontend_queue = conf_frontend.Flag("queue","queue of bioflow").
        Required().
        Short('q').
        Int()
    frontend_opt = conf_frontend.Arg("opt","add or del").
        Required().
        String()

    conf_pstore = config.Command("pipelinestore", "Configure the storeto save pipeine sources")
    pstore_dir = conf_pstore.Arg("dir", "directory to store pipeline sources").
        Required().
        String()

    conf_list_inst = config.Command("lsinst", "List all the instance state")
    cli_queue = conf_list_inst.Arg("queue", "the queue to list").
        Required().
        String()

    conf_delete_inst = config.Command("rminst", "Delete specific instance state")
    cdi_queue = conf_delete_inst.Arg("queue", "The queue of instance").
        Required().
        String()
    cdi_seq = conf_delete_inst.Arg("seq", "The seq of instance").
        Required().
        String()

    conf_list_cluster = config.Command("lscluster", "List mount config for clusters")
    conf_add_cluster = config.Command("addcluster", "Add mount config for storage cluster")
    cac_name = conf_add_cluster.Arg("name", "The name of the cluster").
        Required().
        String()
    cac_fstype = conf_add_cluster.Arg("fstype", "Type of the file system:nfs|alamo|anna|gluster|ceph").
        Required().
        String()
    cac_servers = conf_add_cluster.Arg("servers", "The servers to mount FS: server1,server2").
        Required().
        String()
    cac_opts = conf_add_cluster.Flag("opts", "options for mount").
        Short('o').
        String()
    cac_path = conf_add_cluster.Flag("path", "the target path of NFS server to mount").
        Short('p').
        String()
    cac_vol = conf_add_cluster.Flag("vol", "the volume of the mount config").
        Short('v').
        String()

    conf_rm_cluster = config.Command("rmcluster", "Delete mount config for storage cluster")
    crc_name = conf_rm_cluster.Arg("name", "The name of the cluster").
        Required().
        String()
    crc_vol = conf_rm_cluster.Flag("vol", "the volume of the mount config").
        Short('v').
        String()
    conf_auto_mount = config.Command("automount", "Enable the scheduler auto mount feature")
    conf_disable_mount = config.Command("disablemount", "Disable the scheduler auto mount feature")
    conf_security = config.Command("tunesecurity", "Set security parameters for bioflow")
    cse_param = conf_security.Arg("param", "The params: secmode|maxtasknum|maxjobnum").
        Required().
        String()
    cse_value = conf_security.Arg("value", "The value of the param").
        Required().
        String()
    conf_scheduler = config.Command("tunescheduler", "Set config parameters for job scheduler")
    csc_param = conf_scheduler.Arg("param", "The param of job scheduler").
        Required().
        String()
    csc_value = conf_scheduler.Arg("value", "The value for the param of job scheduler").
        Required().
        String()
    conf_showscheduler = config.Command("showscheduler", "Show config parameters for job scheduler")
    conf_loglevel = config.Command("loglevel", "Set log level for bioflow")
    cll_level = conf_loglevel.Arg("level", "log level: 0(info),1(debug),2(warn),3(error)").
        Required().
        Int()
    cll_queue = conf_loglevel.Flag("queue", "The queue loglevel to set").
        Short('q').
        Int()

    conf_physical = config.Command("physical", "Set config parameters for physical scheduler")
    cpc_param = conf_physical.Arg("param", "The param of the physical scheduler").
        Required().
        String()
    cpc_value = conf_physical.Arg("value", "The value for the param of the physical scheduler").
        Required().
        String()
    conf_datadir = config.Command("setcontainerdatadir", "Set container data dir")
    cdr_datadir = conf_datadir.Arg("dir", "The dir of container data dir").
        Required().
        String()

    /*
     * backend management
     */
    backend = kingpin.Command("backend", "Backend management.")
    bServer = backend.Flag("server", "The bioflow HTTP server address. Default is localhost:9090").
        Short('s').
        String()

    backend_list = backend.Command("list", "List all backends.")
    bl_epoints = backend_list.Flag("endpoints", "ETCD endpoints in format svr1,svr2,svr3").
        String()

    backend_add = backend.Command("add", "Add a backend to scheduler.")
    ba_epoints = backend_add.Flag("endpoints", "ETCD endpoints in format svr1,svr2,svr3").
        String()

    ba_type = backend_add.Flag("type", "Backend type.").
        Short('t').
        Default("paladin").
        Enum("paladin", "kubernetes")
    ba_server = backend_add.Arg("server", "Backend server address.").
        Required().
        String()
    ba_port = backend_add.Arg("port", "Backend port.").
        Required().
        String()

    backend_del = backend.Command("delete", "Delete a backend from scheduler.")
    bd_id = backend_del.Arg("id", "Backend ID.").
        Required().
        String()
    bd_epoints = backend_del.Flag("endpoints", "ETCD endpoints in format svr1,svr2,svr3").
        String()


    backend_quiesce = backend.Command("disable", "Disable a backend.")
    bq_id = backend_quiesce.Arg("id", "Backend ID.").
        Required().
        String()

    backend_resume = backend.Command("enable", "Enable a quiesced backend.")
    br_id = backend_resume.Arg("id", "Backend ID.").
        Required().
        String()

    /*
     * user management
     */
    userCmd = kingpin.Command("user", "User management.")
    uServer = userCmd.Flag("server", "The bioflow HTTP server address. Default is localhost:9090").
        Short('s').
        String()

    user_load = userCmd.Command("load", "Load the users.")
    user_info = userCmd.Command("info", "Show the user info.")
    ui_id = user_info.Arg("id", "User ID.").
        Required().
        String()
    user_list = userCmd.Command("list", "List all the users.")
    user_acct = userCmd.Command("acct", "list users' resource stats.")
    ua_name = user_acct.Flag("name", "Display resource stats of user with the given ID.").
        Short('i').
        Default("").
        String()

    ua_schedtime_range = user_acct.Flag("span", "scheduled time in range between [2017-06-01 12:00:00, 2017-07-01 12:00:00]; use dash(or empty) to set single condition, like [2017-06-01 12:00:00, -].").
        Short('S').
        String()

    user_clracct = userCmd.Command("clracct", "reset users' resource accounting.")
    ul_name = user_clracct.Flag("name", "Display resource stats of user with the given ID.").
        Short('i').
        Default("").
        String()

    ul_schedtime_range = user_clracct.Flag("span", "scheduled time in range between [2017-06-01 12:00:00, 2017-07-01 12:00:00]; use dash(or empty) to set single condition, like [2017-06-01 12:00:00, -].").
        Short('S').
        String()

    user_configure = userCmd.Command("configure", "Configure the user info.")
    uc_id = user_configure.Arg("id", "User ID.").
        Required().
        String()
    uc_credit = user_configure.Flag("credit", "The credit value of user").
        Short('c').
        Default("-1").
        Int64()
    uc_jquota = user_configure.Flag("jobquota", "The job quota of user").
        Short('j').
        Default("-1").
        Int64()
    uc_tquota = user_configure.Flag("taskquota", "The task quota of user").
        Short('t').
        Default("-1").
        Int64()
    uc_mail = user_configure.Flag("mail", "The mail address that receive notice").
        Short('m').
        String()
)

func main() {
    /*Get user information running the CLI*/
    cred := GetClientUserInfo()
    logFilePath := GetUserLogFile()
    /*init the log*/
    config := LoggerConfig{
                Logfile: logFilePath,
            }
    LoggerInit(&config)


    kingpin.CommandLine.HelpFlag.Short('h')

    os := kingpin.Parse()
    cmds := strings.Split(os, " ")

    switch cmds[0] {
    case "auth":
        authCmd := newAuthCommand(*aepoints)
        switch cmds[1] {
        case "passwd":
            authCmd.Passwd(*ag_old, *ag_new)
        case "keygen":
            authCmd.Key(*ak_nu)
        case "keyget":
            authCmd.Key(*ak_tu)
        }
    /*
     * config commands
     */
    case "config":
        configCmd := newConfigCommand()

        switch cmds[1] {
        case "init":
            configCmd.Init(*cepoints, *ci_file)
        case "update-cluster":
            configCmd.UpdateClusterConfig(*cepoints, *cuc_file)
        case "dump":
            configCmd.Dump(*cepoints)
        case "mapvol":
            if *cm_type {
                configCmd.MapServerVol(*cepoints, *cm_map)
            } else {
                configCmd.MapSchedulerVol(*cepoints, *cm_map)
            }
        case "unmapvol":
            if *cu_type {
                configCmd.UnmapServerVol(*cepoints, *cu_map)
            } else {
                configCmd.UnmapSchedulerVol(*cepoints, *cu_map)
            }
        case "jobrootpath":
            configCmd.SetJobRootPath(*cepoints, *cmd_path)
        case "unjobrootpath":
            configCmd.UnSetJobRootPath(*cepoints)
        case "lsinst":
            configCmd.ShowAllInstanceState(*cepoints, *cli_queue)
        case "rminst":
            configCmd.DeleteInstanceState(*cepoints, *cdi_queue, *cdi_seq)
        case "lscluster":
            configCmd.ShowAllClusterMountConfig(*cepoints)
        case "addcluster":
            configCmd.AddStorageClusterConfig(*cepoints, *cac_name, *cac_fstype,
                *cac_servers, *cac_opts, *cac_path, *cac_vol)
        case "rmcluster":
            configCmd.DeleteStorageClusterConfig(*cepoints, *crc_name, *crc_vol)
        case "automount":
            configCmd.SetSchedulerAutoMount(*cepoints, true)
        case "disablemount":
            configCmd.SetSchedulerAutoMount(*cepoints, false)
        case "tunesecurity":
            configCmd.SetSecurityConfig(*cepoints, *cse_param, *cse_value)
        case "tunescheduler":
            configCmd.SetSchedulerConfig(*cepoints, *csc_param, *csc_value)
        case "showscheduler":
            configCmd.ShowSchedulerConfig(*cepoints)
        case "loglevel":
            configCmd.SetLogLevel(*cepoints, *cll_level, *cll_queue)
        case "mailaccount":
            configCmd.SetMail(*cepoints, *mail_user, *mail_pass, *mail_host)
        case "filedownload":
            configCmd.SetFileDownloadConf(*cepoints, *t_cpu, *t_mem)
        case "pipelinestore":
            configCmd.SetPipelineStore(*cepoints, *pstore_dir)
        case "frontend":
            configCmd.SetFrontEnd(*cepoints, *frontend_opt, *frontend_url, *frontend_queue)
        case "physical":
            configCmd.SetPhysicalSchedulerConfig(*cepoints, *cpc_param, *cpc_value)
        case "setcontainerdatadir":
            configCmd.SetContainerDataDir(*cepoints, *cdr_datadir)
        }

    /*
     * backend commands
     */
    case "backend":
        server := ParseBioflowServer(*bServer)
        Client := NewBioflowClient(server, cred)
        backendCmd := newBackendCommand(Client)

        switch cmds[1] {
        case "list":
            backendCmd.List(*bl_epoints)
        case "add":
            backendCmd.Add(*ba_epoints, *ba_type, *ba_server, *ba_port)
        case "delete":
            backendCmd.Delete(*bd_epoints, *bd_id)
        case "disable":
            backendCmd.Disable(*bq_id)
        case "enable":
            backendCmd.Enable(*br_id)
        }

    case "user":
        server := ParseBioflowServer(*uServer)
        Client := NewBioflowClient(server, cred)
        userCmd := newUserCommand(Client)

        switch cmds[1] {
        case "load":
            userCmd.LoadExternalUsers()
        case "info":
            userCmd.ShowUserInfo(*ui_id)
        case "list":
            userCmd.ListAllUsers()
        case "acct":
            args := make(map[string]interface{})
            schedTimeRange := *ua_schedtime_range
            err, start, end := GetTimeStringFromRange(schedTimeRange)
            if err != nil {
                fmt.Printf("Failed to parse span %s\n", err.Error())
                return
            }
            args["user"] = *ua_name
            args["after"] = start
            args["before"] = end
            userCmd.ListUserRscStats(args)

        case "clracct":
            args := make(map[string]interface{})
            schedTimeRange := *ul_schedtime_range
            err, start, end := GetTimeStringFromRange(schedTimeRange)
            if err != nil {
                fmt.Printf("Failed to parse span %s\n", err.Error())
                return
            }
            args["user"] = *ul_name
            args["after"] = start
            args["before"] = end
            userCmd.ResetRscStats(args)

        case "configure":
            userCmd.UpdateUserConfig(*uc_id, *uc_credit, *uc_jquota,
                *uc_tquota, *uc_mail)
        }

    }
}

