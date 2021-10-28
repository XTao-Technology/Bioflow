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
    "strings"
    "fmt"

    "gopkg.in/alecthomas/kingpin.v2"
    . "github.com/xtao/bioflow/client"
    . "github.com/xtao/bioflow/common"
)

var (

    /*
     * client environment command
     */
    env = kingpin.Command("env", "Environment configuration")
    env_set = env.Command("set", "Set environment configurations")
    es_key = env_set.Arg("key", "Key name of environment").
        Required().
        Enum("endpoints", "server")

    es_val = env_set.Arg("value", "Value of environemnt").
        Required().
        String()

    env_get = env.Command("get", "Get environment configurations")
    eg_key = env_get.Flag("key", "Key name of environment").
        Default("all").
        HintOptions("endpoints", "server", "all").
        String()

    /*
     * item commands
     */
    item = kingpin.Command("item", "Manage pipeline items")
    iServer = item.Flag("server", "The bioflow HTTP server address. Default is localhost:9090").
        Short('s').
        String()


    item_add = item.Command("add", "Add a new item")
    ia_file = item_add.Arg("file", "Item description file").
        Required().
        String()
    ia_yaml = item_add.Flag("yaml", "The item file is in Yaml format").
        Short('y').
        Bool()

    item_update = item.Command("update", "Update an existing item")
    iu_file = item_update.Arg("file", "Item description file").
        Required().
        String()
    iu_yaml = item_update.Flag("yaml", "The item file is in Yaml format").
        Short('y').
        Bool()

    item_del = item.Command("delete", "Delete a item")
    id_name = item_del.Arg("name", "Item name being deleted").
        Required().
        String()

    item_ls = item.Command("list", "List items of a pipeline")

    item_info = item.Command("info", "Show information of the item")
    ii_name = item_info.Arg("name", "Item name").
        Required().
        String()

    item_dump = item.Command("dump", "Dump one or more items from server")
    id_itemname = item_dump.Arg("name", "Item name which will be dumped.").
        Required().
        String()
    id_file = item_dump.Arg("file", "File name item will be dumped to.").
        Required().
        String()
    id_yaml = item_dump.Flag("yaml", "The output file is in Yaml format").
        Short('y').
        Bool()

    /*
     * pipeline commands
     */
    pipeline = kingpin.Command("pipeline", "Pipeline management.")
    pServer = pipeline.Flag("server", "The bioflow HTTP server address. Default is localhost:9090").
        Short('s').
        String()

    pipeline_add = pipeline.Command("add", "Add a new pipeline.")
    pa_file = pipeline_add.Arg("file", "pipeline description json file.").
        Required().
        String()
    pa_dir = pipeline_add.Flag("source", "Directory of source file").
        Short('d').
        String()
    pa_yaml = pipeline_add.Flag("yaml", "The input file is in Yaml format").
        Short('y').
        Bool()

    pipeline_update = pipeline.Command("update", "Update a pipeline.")
    pu_file = pipeline_update.Arg("file", "pipeline description json file.").
        Required().
        String()
    pu_dir = pipeline_update.Flag("source", "Directory of source file").
        Short('d').
        String()
    pu_flag = pipeline_update.Flag("force", "Force update a pipeline when it is used by a running job").
        Short('f').
        Bool()
    pu_yaml = pipeline_update.Flag("yaml", "The input file is in Yaml format").
        Short('y').
        Bool()

    pipeline_del = pipeline.Command("delete", "Delete a new pipeline.")
    pd_name = pipeline_del.Arg("name", "Pipeline name.").
        Required().
        String()

    pipeline_ls = pipeline.Command("list", "Show the pipeline list.")

    pipeline_info = pipeline.Command("info", "Show information of specific pipeline.")
    pi_name = pipeline_info.Arg("name", "Pipeline name").
        Required().
        String()
    pi_version = pipeline_info.Flag("version", "Version of the pipeline").
        Short('v').
        Default("head").
        String()

    pipeline_clone = pipeline.Command("clone", "Clone a new pipeline.")
    pc_src = pipeline_clone.Arg("src", "Pipeline name of source.").
        Required().
        String()
    pc_dst = pipeline_clone.Arg("dst", "Pipeline name of destination.").
        Required().
        String()

    pipeline_dump = pipeline.Command("dump", "Dump a pipeline to a json file.")
    pd_pipeline = pipeline_dump.Arg("pipeline", "Pipeline name which will be dumped.").
        Required().
        String()
    pd_file = pipeline_dump.Arg("file", "File name pipeline will be dumped to.").
        Required().
        String()
    pd_version = pipeline_dump.Flag("version", "Version of the pipeline").
        Short('v').
        Default("head").
        String()
    pd_yaml = pipeline_dump.Flag("yaml", "The output file is in Yaml format").
        Short('y').
        Bool()

    pipeline_version = pipeline.Command("version", "List all versions of specific pipeline")
    pi_vers_name = pipeline_version.Arg("name", "Pipeline name").
        Required().
        String()
    /*
     * jobs commands
     */
    jobs = kingpin.Command("jobs", "Jobs management.")
    jsServer = jobs.Flag("server", "The bioflow HTTP server address. Default is localhost:9090").
    Short('s').
    String()

    jobs_ls = jobs.Command("list", "Show jobs info.")
    jsl_all = jobs_ls.Flag("all", "list all jobs running stages").
    Short('a').
    Bool()
    /*
     * job commands
     */
    job = kingpin.Command("job", "Job management.")
    jServer = job.Flag("server", "The bioflow HTTP server address. Default is localhost:9090").
        Short('s').
        String()

    job_submit = job.Command("submit", "Submit a new job.")
    ja_file = job_submit.Arg("file", "Job description json file.").
        Required().
        String()
    ja_yaml = job_submit.Flag("yaml", "Job file is in Yaml format.").
        Short('y').
        Bool()

    job_ls = job.Command("list", "Show job list.")
    jl_all = job_ls.Flag("all", "list running jobs and job history.").
        Short('a').
        Bool()
    jl_old = job_ls.Flag("old", "list only history jobs.").
        Short('o').
        Bool()
    jl_name = job_ls.Flag("name", "filter of job with the given name.").
        Short('i').
        Default("").
        String()
    jl_finish = job_ls.Flag("finish", "flag implies time filter assocates finish time. unset means create time.").
        Short('F').
        Bool()
    jl_span = job_ls.Flag("span", "create/finish time in range between [2017-06-01 12:00:00, 2017-07-01 12:00:00]; use dash(or empty) to set single condition, like [2017-06-01 12:00:00, -].").
        Short('S').
        String()
    jl_pipe = job_ls.Flag("pipeline", "filter of jobs using the given pipeline.").
        Short('p').
        Default("").
        String()
    jl_count = job_ls.Flag("count", "number of jobs being displayed, default list all.").
        Short('n').
        Int()
    jl_priority = job_ls.Flag("priority", "the priority jobs to list, default -1.").
        Short('r').
        Default("-1").
        Int()
    jl_state = job_ls.Flag("state", "the state jobs in it to list, default list all.").
        Short('t').
        Default("").
        String()

    job_cl = job.Command("cleanup", "Cleanup job history")
    jcl_all = job_cl.Flag("all", "cleanup all jobs history.").
        Short('a').
        Bool()
    jcl_name = job_cl.Flag("name", "filter of job with the given name.").
        Short('i').
        Default("").
        String()
    jcl_finish = job_cl.Flag("finish", "flag implies time filter assocates finish time. unset means create time.").
        Short('F').
        Bool()
    jcl_after = job_cl.Flag("after", "filter of jobs create/finish after specific time like 1977-04-09 00:00:00.").
        Short('S').
        Default("").String()
    jcl_before = job_cl.Flag("before", "filter of jobs create/finish before specific time like 1977-01-01 00:00:00.").
        Short('E').
        Default("").String()
    jcl_pipe = job_cl.Flag("pipeline", "filter of jobs using the given pipeline.").
        Short('p').
        Default("").
        String()
    jcl_count = job_cl.Flag("count", "number of jobs to cleanup, default cleanup 0.").
        Short('n').
        Int()
    jcl_priority = job_cl.Flag("priority", "priority of jobs to cleanup, default cleanup all.").
        Short('r').
        Default("-1").
        Int()
    jcl_state = job_cl.Flag("state", "state of jobs to cleanup, default cleanup all.").
        Short('t').
        Default("").
        String()

    job_stat = job.Command("status", "Show the job status.")
    js_id = job_stat.Arg("id", "Job ID.").
        Required().
        String()
    js_detail = job_stat.Flag("detail", "Show details of the job.").
        Short('d').
        Bool()
    js_wait = job_stat.Flag("wait", "Show wait stages of the job.").
        Short('w').
        Bool()
    js_perf = job_stat.Flag("perf", "Show performance data of the job stages.").
        Short('p').
        Bool()
    js_output = job_stat.Flag("output", "Show output of the job.").
        Short('o').
        Bool()

    job_log = job.Command("logs", "Show job error logs.")
    jg_id = job_log.Arg("id", "Job ID.").
        Required().
        String()
    jg_st = job_log.Flag("stage", "Show specific stage log with stage name.").
        Short('T').
        Default("*").
        String()
    jg_st_id = job_log.Flag("stage_id", "Show specific stage log with stage id.").
    	Short('S').
	Default("*").
	String()

    job_resource = job.Command("resource", "Show job task resource usage.")
    jrsc_id = job_resource.Arg("id", "Job ID.").
        Required().
        String()
    jrsc_task_id = job_resource.Arg("taskid", "Task ID.").
        Required().
        String()

    job_pause = job.Command("pause", "Pause a job.")
    jp_id = job_pause.Arg("id", "Job ID.").
        Required().
        String()

    job_kill = job.Command("killtask", "Kill tasks of jobs.")
    jk_id = job_kill.Arg("id", "Job ID.").
        Required().
        String()
    jk_task = job_kill.Flag("task", "Task id try to kill").
        Short('t').
        Default("*").
        String()

    job_resume = job.Command("resume", "Resume a paused job.")
    jr_id = job_resume.Arg("id", "Job ID.").
        Required().
        String()

    job_cancel = job.Command("cancel", "Cancel a running job.")
    jc_id = job_cancel.Arg("id", "Job ID.").
        Required().
        String()

    job_requeue = job.Command("requeue", "Requeue a stage of job.")
    jrq_id = job_requeue.Arg("id", "Job ID.").
        Required().
        String()
    jrq_stage = job_requeue.Arg("stage", "Stage ID").
        Required().
        String()
    jrq_cpu = job_requeue.Flag("cpu", "Update cpu of the stage").
        Short('c').
        String()

    jrq_memory = job_requeue.Flag("memory", "Update memory of the stage").
        Short('m').
        String()

    job_recover = job.Command("recover", "Recover a failed job.")
    jrk_id = job_recover.Arg("id", "Job ID.").
        Required().
        String()

    job_pri = job.Command("setpri", "Set job priority.")
    jpri_id = job_pri.Arg("id", "Job ID.").
        Required().
        String()
    jpri_pri = job_pri.Arg("pri", "The new priority of job.").
        Required().
        Int()

    job_lshang = job.Command("lshangtask", "Show hang jobs and tasks.")

    /*
     * dashboard management
     */
    dashboard = kingpin.Command("dashboard", "Dashboard management.")
    dServer = dashboard.Flag("server", "The bioflow HTTP server address. Default is localhost:9090").
        Short('s').
        String()

    dashboard_show = dashboard.Command("show", "Show the dashboard.")
    ds_v = dashboard_show.Flag("details", "Show more details").
        Short('v').
        Bool()

    /*
     * debug
     */
    debug = kingpin.Command("debug", "Debug operations.")
    dbServer = debug.Flag("server", "The bioflow HTTP server address. Default is localhost:9090").
        Short('s').
        String()

    debug_profile = debug.Command("profile", "Do the profile operation")
    dp_op = debug_profile.Arg("op", "The operation: startcpuprofile|stopcpuprofile|domemprofile").
        Required().
        String()

    debug_faultlinject = debug.Command("faultinject", "Fault inject management")
    faultinject_enable = debug_faultlinject.Command("enable", "enable the fault inject")
    faultinject_disable = debug_faultlinject.Command("disable", "disable the fault inject")

    faultinject_add = debug_faultlinject.Command("inject", "inject a fault")
    fa_id = faultinject_add.Flag("id", "id of fault inject").
        Short('k').
        Default("").
        String()
    fa_type = faultinject_add.Flag("type", "Type of fault inject").
        Short('t').
        Default("").
        String()
    fa_action = faultinject_add.Flag("action", "panic|error|suspend").
        Short('f').
        Default("").
        String()

    faultinject_resume = debug_faultlinject.Command("resume", "inject a fault")
    fr_id = faultinject_resume.Flag("id", "id of fault inject").
        Short('k').
        Default("").
        String()
    fr_type = faultinject_resume.Flag("type", "Type of fault inject").
        Short('t').
        Default("").
        String()
    fr_msg = faultinject_resume.Flag("msg", "resume").
        Short('m').
        Default("").
        String()

    faultinject_delete = debug_faultlinject.Command("delete", "delete a fault")
    fd_id = faultinject_delete.Flag("id", "id of fault inject").
        Short('k').
        Default("").
        String()
    fd_type = faultinject_delete.Flag("type", "Type of fault inject").
        Short('t').
        Default("").
        String()

    faultinject_show = debug_faultlinject.Command("show", "Show fault inject set info")
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
    /*
     * client command
     */
    case "env":
        envCmd := newEnvCommand()

        switch cmds[1] {
        case "get":
            envCmd.Get(*eg_key)
        case "set":
            envCmd.Set(*es_key, *es_val)
        }
    case "dashboard":
        server := ParseBioflowServer(*dServer)
        Client := NewBioflowClient(server, cred)
        dashboardCmd := newDashBoardCommand(Client)

        switch cmds[1] {
        case "show":
            dashboardCmd.Show(*ds_v)
        }
    case "debug":
        server := ParseBioflowServer(*dServer)
        Client := NewBioflowClient(server, cred)
        debugCmd := newDebugCommand(Client)

        switch cmds[1] {
        case "profile":
            debugCmd.DoDebugOperation(*dp_op)
        case "faultinject":
            switch cmds[2] {
            case "enable":
                debugCmd.EnableFaultInject(true)
            case "disable":
                debugCmd.EnableFaultInject(false)
            case "inject":
                debugCmd.InjectFault(*fa_id, *fa_type, *fa_action)
            case "delete":
                debugCmd.DeleteFault(*fd_id, *fd_type)
            case "resume":
                debugCmd.ResumeFault(*fr_id, *fr_type, *fr_msg)
            case "show":
                debugCmd.ShowInjectedFaults()
            }
        }

    /*
     * item commands
     */
    case "item":
        server := ParseBioflowServer(*iServer)
        Client := NewBioflowClient(server, cred)
        itemCmd := newItemCommand(Client)

        switch cmds[1] {
        case "add":
            itemCmd.Add(*ia_file, *ia_yaml)
        case "update":
            itemCmd.Update(*iu_file, *iu_yaml)
        case "list":
            itemCmd.List()
        case "delete":
            itemCmd.Delete(*id_name)
        case "info":
            itemCmd.Info(*ii_name)
        case "dump":
            itemCmd.Dump(*id_itemname, *id_file, *id_yaml)
        }

    /*
     * pipeline commands
     */
    case "pipeline":
        server := ParseBioflowServer(*pServer)
        Client := NewBioflowClient(server, cred)
        pipelineCmd := newPipelineCommand(Client)

        switch cmds[1] {
        case "add":
            pipelineCmd.Add(*pa_file, *pa_dir, *pa_yaml)
        case "delete":
            pipelineCmd.Delete(*pd_name)
        case "list":
            pipelineCmd.List()
        case "info":
            pipelineCmd.Info(*pi_name, *pi_version)
        case "update":
            pipelineCmd.Update(*pu_file, *pu_dir, *pu_flag, *pu_yaml)
        case "clone":
            pipelineCmd.Clone(*pc_src, *pc_dst)
        case "dump":
            pipelineCmd.Dump(*pd_pipeline, *pd_version, *pd_file, *pd_yaml)
        case "version":
            pipelineCmd.Version(*pi_vers_name)
        }
    /*
     * jobs commands
     */
    case "jobs":
        server := ParseBioflowServer(*jsServer)
        Client := NewBioflowClient(server, cred)
        jobsCmd := newJobsCommand(Client)

        switch cmds[1] {
        case "list":
            args := make(map[string]interface{})
            args["All"] = "mem"
            args["Priority"] = -1

            jobsCmd.List(args)
        }
    /*
     * job commands
     */
    case "job":
        server := ParseBioflowServer(*jServer)
        Client := NewBioflowClient(server, cred)
        jobCmd := newJobCommand(Client)

        switch cmds[1] {
        case "submit":
            jobCmd.Submit(*ja_file, *ja_yaml)
        case "cancel":
            jobCmd.Cancel(*jc_id)
        case "status":
            jobCmd.Status(*js_id, *js_detail, *js_wait, *js_perf, *js_output)
        case "pause":
            jobCmd.Pause(*jp_id)
        case "resume":
            jobCmd.Resume(*jr_id)
        case "killtask":
            jobCmd.KillTasks(*jk_id, *jk_task)
        case "list":
            args := make(map[string]interface{})
            if *jl_all == true {
                args["All"] = "all"
            } else if *jl_old == true {
                args["All"] = "old"
            } else {
                args["All"] = "mem"
            }

            args["Name"] = *jl_name
            args["Finish"] = *jl_finish
            timeRange := *jl_span
            err, start, end := GetTimeStringFromRange(timeRange)
            if err != nil {
                fmt.Printf("Failed to parse time range %s\n", err.Error())
                return
            }
            args["After"] = start
            args["Before"] = end
            args["Count"] = *jl_count
            args["Pipeline"] = *jl_pipe
            args["Priority"] = *jl_priority
            args["State"] = *jl_state

            jobCmd.List(args)
        case "cleanup":
            args := make(map[string]interface{})
            if *jcl_all == true {
                args["All"] = "all"
            }
            if strings.TrimSpace(*jcl_name) != "" {
                args["Name"] = *jcl_name
            }
            args["Finish"] = *jcl_finish
            if strings.TrimSpace(*jcl_after) != "" {
                args["After"] = *jcl_after
            }
            if strings.TrimSpace(*jcl_before) != "" {
                args["Before"] = *jcl_before
            }
            args["Count"] = *jcl_count
            if strings.TrimSpace(*jcl_pipe) != "" {
                args["Pipeline"] = *jcl_pipe
            }
            if strings.TrimSpace(*jcl_state) != "" {
                args["State"] = *jcl_state
            }
            args["Priority"] = *jcl_priority

            jobCmd.Cleanup(args)

        case "logs":
            jobCmd.Log(*jg_id, *jg_st, *jg_st_id)
        case "resource":
            jobCmd.Resource(*jrsc_id, *jrsc_task_id)
        case "recover":
            jobCmd.Recover(*jrk_id)
        case "requeue":
            jobCmd.Requeue(*jrq_id, *jrq_stage, *jrq_cpu, *jrq_memory)
        case "setpri":
            jobCmd.UpdateJobPriority(*jpri_id, *jpri_pri)
        case "lshangtask":
            jobCmd.ListHang()
        }
    }
}

