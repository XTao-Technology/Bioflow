package main

import (
    "gopkg.in/alecthomas/kingpin.v2"
    os2 "os"
    "strings"
    . "github.com/xtao/bioflow/common"
)


var (
    xt_chown = kingpin.Command("chown", "Xtao change the owner dir of each FILE.")
    ch_dir = xt_chown.Arg("dir","the change directories").
        Required().
        ExistingDir()
    ch_user = xt_chown.Arg("user","change the dir to the owner user").
        Required().
        String()
    ch_group = xt_chown.Arg("group","change the dir to the owner group").
        Required().
        String()
    ch_uid = xt_chown.Arg("uid","change the dir to the owner uid").
        Required().
        String()
    ch_gid = xt_chown.Arg("gid","change the dir to the owner gid").
        Required().
        String()
    ch_umask = xt_chown.Arg("umask","change the dir to the owner umask").
        Required().
        String()
    ch_ignoredirs = xt_chown.Arg("ignoreDirs","ignore sub dir").
        Required().
        String()


    xt_remove = kingpin.Command("remove", "Xtao delete a dir all files according to pattern")
    xr_dir = xt_remove.Arg("dir", "remove directories").
        Required().
        ExistingDir()

    cl_pattern = xt_remove.Arg("pattern","delete only matching PATTERN").
        Required().
        String()

    xt_delete = kingpin.Command("delete", "Xtao delete a dir")
    xd_dir = xt_delete.Arg("dir", "Delete directories").
        Required().
        ExistingDir()
)

func main() {
    var err error
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
    case "chown":
        err = Chown(*ch_dir, *ch_user, *ch_group, *ch_uid, *ch_gid, *ch_umask, *ch_ignoredirs)
        if err != nil {
            os2.Exit(1)
        }
    case "remove":
        err = CleanupFilesByPatterns(*xr_dir, *cl_pattern)
        if err != nil {
            os2.Exit(1)
        }
    case "delete":
        err = RemoveDir(*xd_dir)
        if err != nil {
            os2.Exit(1)
        }
    }

    return
}