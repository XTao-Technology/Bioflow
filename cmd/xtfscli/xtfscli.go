package main

import (
    "fmt"
    "strings"

    . "github.com/xtao/bioflow/storage"
    . "github.com/xtao/bioflow/common"
)


func Chown(dir string, user string, group string, uid string, gid string, umask string, ignoreDirs string) error {
    fmt.Printf("Chown, dir:%s, user:%s, ignoreDirs:%s start\n",
        dir, user, ignoreDirs)
    ignoreMap := FSUtilsBuildIgnoreDirsTOMap(dir, ignoreDirs)

    u := UserAccountInfo{
        Username: user,
        Groupname: group,
        Uid: uid,
        Gid: gid,
        Umask: umask,
    }

    err := FSUtilsChangeDirUserPrivilege(dir, &u, true, ignoreMap)
    if err != nil {
        fmt.Printf("Fail changediruserprivilege dir %s: %s\n",
            dir, err.Error())
        StorageLogger.Errorf("Fail changediruserprivilege dir %s: %s\n",
            dir, err.Error())
        return err
    }
    fmt.Printf("Chown ends\n")
    return nil
}


func CleanupFilesByPatterns(dir string, pattern string) error {
    fmt.Printf("CleanupFilesByPatterns, dir: %s, patterns:%s start\n",
        dir, pattern)
    patterns := strings.Split(pattern, ",")

    filtratePatterns := make([]string, 0)

    for i := 0; i < len(patterns); i ++ {
        if patterns[i] != "" {
            filtratePatterns = append(filtratePatterns, patterns[i])
        }
    }

    err := FSUtilsDeleteFilesByPatterns(dir, filtratePatterns)
    if err != nil {
        fmt.Printf("Delete files by patterns fail, err: %s\n", err.Error())
        StorageLogger.Errorf("Delete files by patterns fail, err: %s\n", err.Error())
    }
    fmt.Printf("CleanupFilesByPatterns ends\n")
    return err
}

func RemoveDir(path string) error {
    fmt.Printf("RemoveDir, path: %s start\n", path)
    err, fsPath := GetStorageMgr().MapPathToSchedulerVolumeMount(path)
    if err != nil {
        fmt.Printf("Failed to remove directory path: %s for job, err: %s\n",
            path, err.Error())
        SchedulerLogger.Errorf("Failed to remove directory path: %s for job, err: %s",
            path, err.Error())
        return err
    }
    err = FSUtilsDeleteDir(fsPath, true)
    if err != nil {
        fmt.Printf("Failed to remove directory for job %s\n", err.Error())
        SchedulerLogger.Errorf("Failed to remove directory for job %s", err.Error())
    } else {
        fmt.Printf("Success to remove directory for job\n")
        SchedulerLogger.Infof("Remove directory for job")
    }
    fmt.Printf("RemoveDir, ends\n")
    return err
}