/* 
 Copyright (c) 2017-2018 XTAO technology <www.xtaotech.com>
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
    "strings"
    "errors"
    "fmt"

    "github.com/xtao/bioflow/scheduler/backend"
    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/storage"
)

var (
    ErrInvalidLogFileNameFormat error = errors.New("Invalid Job log file name")
)

const (
    JOB_STDOUT_EXT string = "info"
    JOB_STDERR_EXT string = "err"
    JOB_PROFILING_EXT string = "profiling"
    JOB_CMD_STDOUT_EXT string = "cmd_info"
    JOB_CMD_STDERR_EXT string = "cmd_err"
)

func MapSandboxFileToExt(sandboxFile string) string {
    switch sandboxFile {
        case backend.SANDBOX_STDOUT:
            return JOB_STDOUT_EXT
        case backend.SANDBOX_STDERR:
            return JOB_STDERR_EXT
        case backend.SANDBOX_PROFILING:
            return JOB_PROFILING_EXT
        case backend.SANDBOX_CMD_STDERR:
            return JOB_CMD_STDERR_EXT
        case backend.SANDBOX_CMD_STDOUT:
            return JOB_CMD_STDOUT_EXT
        default:
            return "unknown_type"
    }
}

/*
 * Define the job logs file name format. Different kinds of job log
 * files are formated as follows:
 * 1) stdout: {stagename}-{taskname}.info
 * 2) stderr: {stagename}-{taskname}.err
 * 3) profiling: {stagename}-{taskname}.profiling
 *
 */
type JobLogFileName struct {
    name string
    ext string
}

func ParseJobLogFileName(fileName string) (*JobLogFileName, error) {
    items := strings.Split(fileName, ".")
    if len(items) < 2 {
        return nil, ErrInvalidLogFileNameFormat
    }
    ext := items[len(items) - 1]
    name := strings.TrimSuffix(fileName,
        fmt.Sprintf(".%s", ext))

    return &JobLogFileName{name: name, ext: ext}, nil
}

func BuildJobLogFileName(stageName string, taskId string) string {
    return fmt.Sprintf("%s-%s", stageName, taskId)
}

func NewJobLogFileName(stageName string, taskId string, ext string) *JobLogFileName {
    return &JobLogFileName{
        name: BuildJobLogFileName(stageName, taskId),
        ext: ext,
    } 
}

func (jlf *JobLogFileName) FileName() string {
    return fmt.Sprintf("%s.%s", jlf.name, jlf.ext)
}

func (jlf *JobLogFileName) IsLogFile() bool {
    if jlf.ext == JOB_STDOUT_EXT || jlf.ext == JOB_STDERR_EXT ||
        jlf.ext == JOB_CMD_STDERR_EXT || jlf.ext == JOB_CMD_STDOUT_EXT {
        return true
    }

    return false
}

func (jlf *JobLogFileName) IsProfilingFile() bool {
    if jlf.ext == JOB_PROFILING_EXT {
        return true
    }

    return false
}

func (jlf *JobLogFileName) ParseCanonicalFormat(stageName string) (bool, string) {
    /*stage's log file should prex with stage name*/
    prefixName := fmt.Sprintf("%s-", stageName)
    if !strings.HasPrefix(jlf.name, prefixName) {
        return false, ""
    }

    return true, strings.TrimPrefix(jlf.name, prefixName) + "." + jlf.ext
}

func (jlf *JobLogFileName) IsTaskFile(taskId string) bool {
    return strings.HasSuffix(jlf.name, taskId)
}

func (jlf *JobLogFileName) Name() string {
    return jlf.name
}


/*
 * The job file set mantains the internal files associated
 * with a job:
 * 1) log files: stdout, stderr
 * 2) profiling files
 *
 */
type jobFileSet struct {
    fileSetDir string
}

func NewJobFileSet(dir string) *jobFileSet {
    return &jobFileSet {
        fileSetDir: dir,
    }
}


/* 
 * Get stderr or stdout logs of stages with the name.
 */
func (fs *jobFileSet) ReadStageLogs(stageName string) (error, map[string]string) {
    storageMgr := GetStorageMgr()
    err, dir := storageMgr.MapPathToSchedulerVolumeMount(fs.fileSetDir)
    if err != nil {
        SchedulerLogger.Errorf("Failed to map dir %s on scheduler\n",
            fs.fileSetDir)
        return err, nil
    }

    err, files := FSUtilsReadDirFiles(dir)
    if err != nil {
        SchedulerLogger.Errorf("Failed to read file set dir %s, error: %s\n",
            dir, err.Error())
        return err, nil
    }

    logs := make(map[string]string)
    /*filter the log files by stage name*/
    for _, fileName := range files {
        jlf, err := ParseJobLogFileName(fileName)
        if err != nil {
            SchedulerLogger.Errorf("Fail to parse job log file name %s: %s\n",
                fileName, err.Error())
            continue
        }

        if !jlf.IsLogFile() {
            continue
        }

        if ok, taskId := jlf.ParseCanonicalFormat(stageName); ok {
            path := fmt.Sprintf("%s/%s", dir, fileName)
            log, err := FSUtilsReadFile(path)
            if err != nil {
                SchedulerLogger.Errorf("Fail read log file %s: %s\n",
                    path, err.Error())
                logs[taskId] = "Fail read task " + taskId + " log: " + err.Error()
            } else {
                SchedulerLogger.Debugf("Successfully read log file %s\n",
                    path)
                logs[taskId] = string(log)
            }
        }
    }

    return nil, logs
}

func (fs *jobFileSet) ReadTaskLogs(stageName string, taskIds []string) (error, map[string]string) {
    storageMgr := GetStorageMgr()
    err, dir := storageMgr.MapPathToSchedulerVolumeMount(fs.fileSetDir)
    if err != nil {
        SchedulerLogger.Errorf("Failed to map dir %s on scheduler\n",
            fs.fileSetDir)
        return err, nil
    }

    logs := make(map[string]string)
    for _, taskId := range taskIds {
        for _, ext := range []string {JOB_STDOUT_EXT, JOB_STDERR_EXT, JOB_CMD_STDOUT_EXT, JOB_CMD_STDERR_EXT} {
            jlf := NewJobLogFileName(stageName, taskId, ext)
            path := fmt.Sprintf("%s/%s", dir, jlf.FileName())
            log, err := FSUtilsReadFile(path)
            logId := taskId + "." + ext
            if err != nil {
                SchedulerLogger.Errorf("Fail read log file %s: %s\n",
                    path, err.Error())
                logs[logId] = "Fail read task " + taskId + " log: " + err.Error()
            } else {
                SchedulerLogger.Debugf("Successfully read log file %s\n",
                    path)
                logs[logId] = string(log)
            }
        }
    }

    return nil, logs
}

func (fs *jobFileSet) GenerateTaskSandboxFilePath(stageName string, taskId string,
    sandboxFile string) (string, error) {
    ext := MapSandboxFileToExt(sandboxFile)
    jfName := NewJobLogFileName(stageName, taskId, ext)

    storageMgr := GetStorageMgr()
    err, dir := storageMgr.MapPathToSchedulerVolumeMount(fs.fileSetDir)
    if err != nil {
        SchedulerLogger.Errorf("Failed to map dir %s on scheduler\n",
            fs.fileSetDir)
        return "", err
    }

    return dir + "/" + jfName.FileName(), nil
}

func (fs *jobFileSet) SaveTaskSandboxFile(stageName string, taskId string,
    sandboxFile string, buf []byte) error {
    filePath, err := fs.GenerateTaskSandboxFilePath(stageName, taskId, sandboxFile)
    if err != nil {
        return err
    }

    return FSUtilsWriteFile(filePath, buf)
}

func (fs *jobFileSet) GetProfilingFileByStageTask(stageName string, taskId string) (string, error) {
    jfName := NewJobLogFileName(stageName, taskId, JOB_PROFILING_EXT)

    storageMgr := GetStorageMgr()
    err, dir := storageMgr.MapPathToSchedulerVolumeMount(fs.fileSetDir)
    if err != nil {
        SchedulerLogger.Errorf("Failed to map dir %s on scheduler\n",
            fs.fileSetDir)
        return "", err
    }

    path := dir + "/" + jfName.FileName()
    _, err = FSUtilsStatFile(path)

    return path, err
}

func (fs *jobFileSet) GetTaskProfilingFiles(taskId string) (map[string]string, error) {
    storageMgr := GetStorageMgr()
    err, dir := storageMgr.MapPathToSchedulerVolumeMount(fs.fileSetDir)
    if err != nil {
        SchedulerLogger.Errorf("Failed to map dir %s on scheduler\n",
            fs.fileSetDir)
        return nil, err
    }

    err, files := FSUtilsReadDirFiles(dir)
    if err != nil {
        SchedulerLogger.Errorf("Failed to read file set dir %s, error: %s\n",
            dir, err.Error())
        return nil, err
    }

    fileMap := make(map[string]string)
    /*filter the log files by stage name*/
    for _, fileName := range files {
        jlf, err := ParseJobLogFileName(fileName)
        if err != nil {
            SchedulerLogger.Errorf("Fail to parse job log file name %s: %s\n",
                fileName, err.Error())
            continue
        }

        if !jlf.IsProfilingFile() {
            continue
        }

        if !jlf.IsTaskFile(taskId) {
            continue
        }

        fileMap[jlf.Name()] = dir + "/" + fileName
    }

    return fileMap, nil
}
