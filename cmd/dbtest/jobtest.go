package main

import (
    "github.com/xtao/bioflow/dbservice"
    "fmt"
    "time"
    )


func main() {
    db := dbservice.NewDBService("bioflow", "bioflow", "bioflow", "postgres")

    tm := time.Now()

    job := &dbservice.JobInfo{
        Id: 1,
        Name: "test-job-1",
        Json: "this is json",
        Created: tm.Format(time.UnixDate),
        State: "Created",
        Pipeline: "pipeline-1",
        WorkDir: "jobs/job1",
    }

    err := db.AddJob(job)
    if err != nil {
        fmt.Printf("Add job fail %s \n", err.Error())
    }

    err, jobs := db.GetJobById(1)
    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Println(jobs)
    }

    err, ids := db.GetAllJobIds()
    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Println(ids)
    }

    job.State = "Finished"
    err = db.UpdateJob(job)
    if err != nil {
        fmt.Println(err)
    }

    err, jobs = db.GetJobById(1)
    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Println(jobs)
    }

    err = db.DeleteJob(1)
    if err != nil {
        fmt.Println(err)
    }

    err, jobs = db.GetJobById(1)
    if err != nil {
        fmt.Println(err)
    } else {
        fmt.Println(jobs)
    }
}
