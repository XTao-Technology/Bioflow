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
package storage

import (
    "strings"
    "os"
    "syscall"
    "path/filepath"
    "errors"
    "io/ioutil"
    "io"
    "bufio"
    "strconv"
    "fmt"
    "time"

    . "github.com/xtao/bioflow/common"
    xcommon "github.com/xtao/xstone/common"
    xstorage "github.com/xtao/xstone/storage"
    xconfig "github.com/xtao/xstone/config"
    "github.com/xtao/bioflow/blparser"
    "github.com/xtao/bioflow/storage/hdfsutils"
    )

/* Judge the file type by check the prefix:
 * 1) hdfs:// : a hdfs file
 * 2) other: a normal file
 */
func FSUtilsIsHDFSPath(path string) bool {
    if strings.HasPrefix(path, "hdfs:") {
        return true
    }

    return false
}

/*each hdfs file should be named: hdfs@clustername:path*/
func FSUtilsIsHDFSVol(vol string, cluster string) bool {
    if strings.ToUpper(vol) == "HDFS" {
        return true
    }

    return false
}

func FSUtilsSplitPathType(path string) (string, string) {
    extType := ""
    _, file := FSUtilsSplitPath(path)
    if file != "" {
        fileItems := strings.Split(file, ".")
        file = ""
        for i := 0; i < len(fileItems) - 1; i ++ {
            if file == "" {
                file = fileItems[i]
            } else if fileItems[i] != "" {
                file += "." + fileItems[i]
            }
        }
        extType = fileItems[len(fileItems) - 1]
    }

    return file, extType
}

func FSUtilsSplitPath(path string) (string, string) {
    pathItems := strings.Split(path, "/")
    dirIndex := len(pathItems) - 2
    file := pathItems[dirIndex + 1]
    if file == "" {
        if len(pathItems) >= 2 {
            dirIndex := len(pathItems) - 3
            file = pathItems[dirIndex + 1] 
        }
    }

    dir := "/"
    for i := 0; i <= dirIndex; i ++ {
        dir += pathItems[i] + "/"
    }

    return dir, file
}

func FSUtilsRemoveFileDotSuffix(fileName string, removeAll bool) string {
    fileItems := strings.Split(fileName, ".")
    len := len(fileItems)
    if len > 1 {
        if removeAll {
            return fileItems[0]
        } else {
            file := fileItems[0]
            for i := 1; i < len - 1; i ++ {
                file += "." + fileItems[i]
            }
            return file
        }
    } else if len == 1 {
        return fileItems[0]
    } else {
        return ""
    }
}

func FSUtilsDeleteFilesByPatterns2(workDir string, patterns []string) error {
    err, files := FSUtilsReadDirFiles(workDir)
    if err != nil {
        StorageLogger.Errorf("Fail to get directory %s files: %s\n",
            workDir, err.Error())
        return err
    }

	blParser := blparser.NewBLParser()
	if blParser == nil {
		StorageLogger.Errorf("Can't create bl parser\n")
		return errors.New("Can't create bl parser")
	}
	
	deleteFlag := make(map[int]bool)
    for i := 0; i < len(patterns); i ++ {
        pattern := patterns[i]
        for j := 0; j < len(files); j ++ {
			if deleted, ok := deleteFlag[j]; ok && deleted {
				continue
			}
            if blparser.BLUtilsMatchStringByPattern(files[j],
				pattern) {
                StorageLogger.Debugf("Delete file %s by match pattern %s\n",
                    files[j], pattern)

                var preTime time.Time 
                perfStats.StartProfileDeleteFile(&preTime)
				err = os.Remove(workDir + "/" + files[j])
                perfStats.EndProfileDeleteFile(preTime)

				if err != nil {
					StorageLogger.Errorf("Fail to delete file %s: %s\n",
						files[j], err.Error())
                    GetDashBoard().UpdateStats(STAT_OP_REMOVE_FILE_FAIL,
                        1)
				} else {
					StorageLogger.Debugf("Successfully to delete file %s\n",
						files[j])
					deleteFlag[j] = true
                    GetDashBoard().UpdateStats(STAT_OP_REMOVE_FILE,
                        1)
				}
            }
        }
    }

    return nil
}

func FSUtilsDeleteFile(file string) error {
    var preTime time.Time
    perfStats.StartProfileDeleteFile(&preTime)
	err := os.Remove(file)
    perfStats.EndProfileDeleteFile(preTime)
	if err != nil {
		StorageLogger.Errorf("Delete file %s error: %s\n",
			file, err.Error())
        GetDashBoard().UpdateStats(STAT_OP_REMOVE_FILE_FAIL, 1)
		return err
	} else {
        GetDashBoard().UpdateStats(STAT_OP_REMOVE_FILE, 1)
    }
	return nil
}

func FSUtilsReadDirItems(dirPath string) ([]os.FileInfo, error) {
    if !FSUtilsIsHDFSPath(dirPath) {
        return ioutil.ReadDir(dirPath)
    } else {
        return hdfsutils.ReadDir(dirPath)
    }
}

func FSUtilsReadDirFiles(dirPath string) (error, []string) {
    files := make([]string, 0)
    var preTime time.Time 
    perfStats.StartProfileReadDir(&preTime)
    dirItems, err := FSUtilsReadDirItems(dirPath)
    perfStats.EndProfileReadDir(preTime)
    if err != nil {
        StorageLogger.Errorf("Read directory %s error %v\n", dirPath, err)
        GetDashBoard().UpdateStats(STAT_OP_READDIR_FAIL, 1)
        return err, nil
    } else {
        for _, file := range dirItems {
            if !file.IsDir() {
                files = append(files, file.Name())
            }
        }
        GetDashBoard().UpdateStats(STAT_OP_READDIR, 1)
    }

    return nil, files
}

/*
 * Parse URI format path like "volname@clustername:pathname" to
 * clustername, volname, path
 */
func FSUtilsParseFileURI(path string) (error, string, string, string) {
    strItems := strings.Split(path, ":")
    if len(strItems) <= 1 {
        return errors.New("Invalid filepath format"), "", "", ""
    } else {
        volLoc := strItems[0]
        volItems := strings.Split(volLoc, "@")
        if len(volItems) == 1 {
            return nil, "", volItems[0], strings.TrimPrefix(path, strItems[0] + ":")
        } else if len(volItems) == 2 {
            return nil, volItems[1], volItems[0], strings.TrimPrefix(path, strItems[0] + ":")
        } else {
            return errors.New("Invalid vol cluster path"), "", "", ""
        }
    }
}

/*Check whether is a regular URI*/
func FSUtilsIsFileURI(path string) bool {
    if err,_,_,_ := FSUtilsParseFileURI(path); err != nil {
        return false
    }

    return true
}

/*utils to build volume URI vol@cluster*/
func FSUtilsBuildVolURI(cluster string, vol string) string {
    return xcommon.FSCommonUtilsBuildVolURI(cluster, vol)
}

/*
 * utils to build volume mountpoint, we avoid the special characters
 * here, but use "-" as separator
 */
func FSUtilsBuildVolMountPoint(cluster string, vol string) string {
    if cluster == "" {
        return fmt.Sprintf("default-%s", vol)
    } else {
        return cluster + "-" + vol
    }
}

func FSUtilsParseVolURI(volURI string) (error, string, string) {
    volItems := strings.Split(volURI, "@")
    if len(volItems) == 1 {
        return nil, "", volItems[0]
    } else if len(volItems) == 2 {
        return nil, volItems[1], volItems[0]
    } else {
        return errors.New("Invalid Vol URI " + volURI),
            "", ""
    }
}

func FSUtilsWriteFile(path string, b []byte) error {
    var preTime time.Time
    perfStats.StartProfileWriteFile(&preTime)
    defer perfStats.EndProfileWriteFile(preTime)

    if !FSUtilsIsHDFSPath(path) {
        return ioutil.WriteFile(path, b, 0644)
    } else {
        return hdfsutils.PutFile(path, b)
    }
}

func FSUtilsCopyFile(srcPath string, targetPath string) error {
    var preTime time.Time
    perfStats.StartProfileWriteFile(&preTime)
    defer perfStats.EndProfileWriteFile(preTime)

    if !FSUtilsIsHDFSPath(srcPath) {
        srcFile, err := os.Open(srcPath)
        if err != nil {
            return err
        }
        defer srcFile.Close()

        dstFile, err := os.OpenFile(targetPath, os.O_RDWR|os.O_CREATE, 0755)
        if err != nil {
            return err
        }
        defer dstFile.Close()

        _, err = io.Copy(dstFile, srcFile)
        return err
    } else {
        return errors.New("hdfs not support copy operation")
    }
}

func FSUtilsReadFile(path string) (out []byte, err error) {
    var preTime time.Time
    perfStats.StartProfileReadFile(&preTime)
    defer perfStats.EndProfileReadFile(preTime)

    if !FSUtilsIsHDFSPath(path) {
	    return ioutil.ReadFile(path)
    } else {
        return hdfsutils.ReadFile(path)
    }
}

func FSUtilsCountRetryNum() {
    perfStats.ProfileRetryCount()
}

func FSUtilsMkdir(path string, recursive bool) error {
    StorageLogger.Debugf("FSUtilsMkdir: %s\n", path)
    var err error
    if recursive {
        oldMask := syscall.Umask(0)
        
        var preTime time.Time
        perfStats.StartProfileMkDirPath(&preTime)
        if FSUtilsIsHDFSPath(path) {
            err = hdfsutils.MkDir(path, true)
        } else {
            err = os.MkdirAll(path, os.ModePerm)
        }
        perfStats.EndProfileMkDirPath(preTime)

        syscall.Umask(oldMask)
    } else {
        oldMask := syscall.Umask(0)

        var preTime time.Time
        perfStats.StartProfileMkDir(&preTime)
        if FSUtilsIsHDFSPath(path) {
            err = hdfsutils.MkDir(path, false)
        } else {
            err = os.Mkdir(path, os.ModePerm)
        }
        perfStats.EndProfileMkDir(preTime)
        
        syscall.Umask(oldMask)
    }

    if err != nil {
        GetDashBoard().UpdateStats(STAT_OP_MKDIR_FAIL, 1)
    } else {
        GetDashBoard().UpdateStats(STAT_OP_MKDIR, 1)
    }

    return err
}

func FSUtilsReadLines(path string) (error, []string) {
    if FSUtilsIsHDFSPath(path) {
        return errors.New("Don't support read HDFS file"), nil
    }

    rw, err := os.Open(path)
    if err != nil { 
        return err, nil
    }
    defer rw.Close()
    
    strLines := make([]string, 0)
    rb := bufio.NewReader(rw)
    for {
        var preTime time.Time
        perfStats.StartProfileReadFile(&preTime)
        line, _, err := rb.ReadLine()
        perfStats.EndProfileReadFile(preTime)

        if err == io.EOF {
            break
        }
        /*filter-out whitespaces*/
        strLine := string(line)
        strLine = strings.TrimSpace(strLine)
        if strLine != "" {
            strLines = append(strLines, strLine)
        }
    }

    return nil, strLines
}

func FSUtilsBuildIgnoreDirsTOMap(dir string, ignoreDirs string) map[string]bool {
    resMap := make(map[string]bool)
    ignoreDirsSlice := strings.Split(ignoreDirs, ";")
    if len(ignoreDirsSlice) <= 0 {
        return resMap
    }

    for i := 0; i < len(ignoreDirsSlice); i ++ {
        key := dir + "/" + ignoreDirsSlice[i]
        newKey := strings.Replace(key, "//", "/", -1)
        resMap[newKey] = true
    }

    return resMap
}

func FSUtilsChangeDirUserPrivilege(dirPath string,
    accountInfo *UserAccountInfo, recursive bool, ignoreDirsMap map[string]bool) error {
    if FSUtilsIsHDFSPath(dirPath) {
        if accountInfo == nil {
            return errors.New("No valid account info")
        }

        err := hdfsutils.Chown(dirPath, accountInfo.Username, accountInfo.Groupname,
            recursive)
        if err != nil {
            StorageLogger.Errorf("Fail chown hdfs file %s: %s\n",
                dirPath, err.Error())
            return err
        }
        userMask, err := strconv.ParseUint(accountInfo.Umask, 10, 32)
        if err != nil {
            return err
        }

        umode := uint32(userMask) ^ uint32(os.ModePerm)
        err = hdfsutils.Chmod(dirPath, uint(umode), recursive)
        if err != nil {
            StorageLogger.Errorf("Fail to chmod hdfs file %s: %s\n",
                dirPath, err.Error())
        }
        return err
    }

    err := FSUtilsChangeFileUserPrivilege(dirPath,
        accountInfo)
    if err != nil {
        StorageLogger.Errorf("Can't set %s correct privilege: %s\n",
            dirPath, err.Error())
        return err
    }

    if !recursive {
        return nil
    }

    /*Recursive fix sub directory*/
    fileItems, err := FSUtilsReadDirItems(dirPath)
    if err != nil {
        StorageLogger.Errorf("Read directory %s error %v\n",
            dirPath, err)
        return err
    }

    newPath := strings.Replace(dirPath, "//", "/", -1)
    if _, ok := ignoreDirsMap[newPath]; ok {
        StorageLogger.Debugf("FSUtilsChangeDirUserPrivilege(directory:%s is ignore dir)\n",
            dirPath)
        return nil
    }

    for i := 0; i < len(fileItems); i ++ {
        item := fileItems[i]
        path := dirPath + "/" + item.Name()
        if item.IsDir() && item.Name() != "" {
            err = FSUtilsChangeDirUserPrivilege(path,
                accountInfo, recursive, ignoreDirsMap)
        } else {
            err = FSUtilsChangeFileUserPrivilege(path,
                accountInfo)
        }
        if err != nil {
            StorageLogger.Errorf("Read directory %s error %v, ignore it\n",
                dirPath, err)
            fmt.Printf("Read directory %s error: %s, ignore it\n",dirPath, err.Error())
        }
    }

    return nil
}

func FSUtilsChangeFileUserPrivilege(filePath string,
    accountInfo *UserAccountInfo) error {
    if accountInfo == nil {
        return errors.New("No valid account info")
    }

    uid, err := strconv.Atoi(accountInfo.Uid)
    if err != nil {
        return err
    }
    gid, err := strconv.Atoi(accountInfo.Gid)
    if err != nil {
        return err
    }
    userMask, err := strconv.ParseUint(accountInfo.Umask, 10, 32)
    if err != nil {
        return err
    }

    var preTime time.Time
    perfStats.StartProfileChown(&preTime)
    err = syscall.Chown(filePath, uid, gid)
    perfStats.EndProfileChown(preTime)
    if err != nil {
        StorageLogger.Errorf("Can't chown file %s to %d:%d: %s\n",
            filePath, uid, gid, err.Error())
        GetDashBoard().UpdateStats(STAT_OP_CHOWN_FAIL, 1)
        return err
    } else {
        GetDashBoard().UpdateStats(STAT_OP_CHOWN, 1)
    }

    umode := uint32(userMask) ^ uint32(os.ModePerm)
    perfStats.StartProfileChmod(&preTime)
    err = syscall.Chmod(filePath, umode)
    perfStats.EndProfileChmod(preTime)
    if err != nil {
        StorageLogger.Errorf("Can't chmod file %s to %d: %s\n",
            filePath, umode, err.Error())
        GetDashBoard().UpdateStats(STAT_OP_CHMOD_FAIL, 1)
        return err
    } else {
        GetDashBoard().UpdateStats(STAT_OP_CHMOD, 1)
    }

    return nil
}
    
func FSUtilsRemoveFile(path string) error {
    if FSUtilsIsHDFSPath(path) {
        return hdfsutils.RmFile(path, false, false)
    } else {
        return os.Remove(path)
    }
}

func FSUtilsCleanupEmptyDirectory(dirPath string,
    recursive bool) error {
    nonEmptyErr := errors.New("File exist")
    dirEmpty := true
    /*Recursive fix sub directory*/
    fileItems, err := FSUtilsReadDirItems(dirPath)
    for i := 0; i < len(fileItems); i ++ {
        item := fileItems[i]
        if item.IsDir() && item.Name() != "" {
            path := dirPath + "/" + item.Name()
            subErr := FSUtilsCleanupEmptyDirectory(path, recursive)
            if subErr != nil {
                /*a sub directory not cleanup*/
                dirEmpty = false
            }
        } else {
            /*exist a file*/
            dirEmpty = false
        }
    }

    if dirEmpty {
        var preTime time.Time
        perfStats.StartProfileRmDir(&preTime)
        err = FSUtilsRemoveFile(dirPath)
        perfStats.EndProfileRmDir(preTime)
        if err == nil {
            StorageLogger.Debugf("Succeed to delete directory %s\n",
                dirPath)
            GetDashBoard().UpdateStats(STAT_OP_RMDIR, 1)
        }
    } else {
        err = nonEmptyErr
    }

    return err
}

func FSUtilsRename(oldPath string, targetPath string) error {
    var preTime time.Time
    perfStats.StartProfileRename(&preTime)
    defer perfStats.EndProfileRename(preTime)

    var err error = nil
    if FSUtilsIsHDFSPath(oldPath) {
        err = hdfsutils.Rename(oldPath, targetPath)
    } else {
        err = syscall.Rename(oldPath, targetPath)
    }

    return err
}

/*
 * Rename the files or directories in the old directory to the new
 * directory with the same base name. If there is a same named directory
 * under the target directory, rename will fail. If the dupPrefix is set,
 * it will retry rename the file adding a prefix to avoid the duplicate name
 */
func FSUtilsMergeDirFiles(oldDirPath string, newDirPath string,
    dupPrefix string) error {
    var preTime time.Time

    /*
     * At the time of recovery, some stages have already performed PostExecution,
     * and shadow workdir has been merged into the working directory and has been
     * removed, so the following repeat operations do not need to continue.
     */
    _, err := FSUtilsStatFile(oldDirPath)
    if err != nil {
        /*
         * The shadow workdir is not exist, return directly.
         */
        if os.IsNotExist(err) {
            StorageLogger.Infof("The shadow workdir is not exist, maybe we already performed PostExecution")
            return err
        } else {
            StorageLogger.Errorf("Stat shadow workdir failed: %s",
                err.Error())
            return err
        }
    }

    perfStats.StartProfileReadDir(&preTime)
    dirItems, err := FSUtilsReadDirItems(oldDirPath)
    perfStats.EndProfileReadDir(preTime)
    if err != nil {
        StorageLogger.Errorf("Read old directory %s error %s\n",
            oldDirPath, err.Error())
        GetDashBoard().UpdateStats(STAT_OP_READDIR_FAIL, 1)
        return err
    } else {
        StorageLogger.Debugf("Read old directory %s success and get %d files, then begin merge it to new dir: %s\n",
            oldDirPath, len(dirItems), newDirPath)
        for _, file := range dirItems {
            /*merge the file to target directory*/
            oldPath := oldDirPath + "/" + file.Name()
            targetPath := newDirPath + "/" + file.Name()
            err = FSUtilsRename(oldPath, targetPath)
            if err != nil {
                StorageLogger.Errorf("Fail to rename %s to %s: %s\n",
                    oldPath, targetPath, err.Error())
                /*try to rename to a prefixed new path*/
                if dupPrefix != "" {
                    targetPath = newDirPath + "/" + strings.TrimSpace(dupPrefix) + "-" + file.Name()
                    StorageLogger.Debugf("Try to rename %s to a prefixed path %s\n",
                        oldPath, targetPath)
                    err = FSUtilsRename(oldPath, targetPath)
                }
            }

            if err != nil {
                StorageLogger.Errorf("Fail to rename %s to %s: %s\n",
                    oldPath, targetPath, err.Error())
                GetDashBoard().UpdateStats(STAT_OP_RENAME_FAIL, 1)
            } else {
                StorageLogger.Debugf("Succeed to rename %s to %s\n",
                    oldPath, targetPath)
                GetDashBoard().UpdateStats(STAT_OP_RENAME, 1)
            }
        }
        GetDashBoard().UpdateStats(STAT_OP_READDIR, 1)
    }

    return nil
}

func FSUtilsDeleteDir(dirPath string, recursive bool) error {
    if FSUtilsIsHDFSPath(dirPath) {
        return hdfsutils.RmFile(dirPath, recursive, false)
    }

    var preTime time.Time
    perfStats.StartProfileReadDir(&preTime)
    dirItems, err := ioutil.ReadDir(dirPath)
    perfStats.EndProfileReadDir(preTime)
    if err != nil {
        StorageLogger.Errorf("Read directory %s error %s\n",
            dirPath, err.Error())
        GetDashBoard().UpdateStats(STAT_OP_READDIR_FAIL, 1)
        return err
    } else {
        GetDashBoard().UpdateStats(STAT_OP_READDIR, 1)
        StorageLogger.Debugf("In func FSUtilsDeleteDir Read directory %s success and get %d files, then begin delete it\n",
            dirPath, len(dirItems))
        for _, file := range dirItems {
            /*merge the file to target directory*/
            filePath := dirPath + "/" + file.Name()
            if file.IsDir() {
                if !recursive {
                    return errors.New("Child directories exist")
                }

                err = FSUtilsDeleteDir(filePath, recursive)
                if err != nil {
                    StorageLogger.Errorf("Fail to remove dir %s: %s\n",
                        filePath, err.Error())
                    return err
                } else {
                    StorageLogger.Debugf("Succeed to remove dir %s\n",
                        filePath)
                }
            } else {
                err = FSUtilsDeleteFile(filePath)
                if err != nil {
                    return err
                }
            }
        }
    }

    return FSUtilsDeleteFile(dirPath)
}

func FSUtilsMirrorPathToCluster(filePath string, vol string, cluster string) (error, string) {
    err, _, _, pathFile := FSUtilsParseFileURI(filePath)
    if err != nil {
        return err, ""
    }

    if vol == "" {
        return errors.New("Volume shouldn't be empty"), ""
    }

    volURI := FSUtilsBuildVolURI(cluster, vol)
    return nil, volURI + ":" + pathFile
}


/*
 * Get the shortest path which not exist. If the path itself exists, return
 * false. otherwise return true and the found path.
 */
func FSUtilsGetShortestNonExistPath(filePath string) (bool, string, error) {
    if FSUtilsIsHDFSPath(filePath) {
        return hdfsutils.GetShortestNonExistPath(filePath)
    }

    dir := filepath.Dir(filePath)
    if dir == "." {
        return true, "", errors.New("Empty path")
    }
    _, err := os.Stat(filePath)
    if err == nil {
        return false, "", nil
    }

    lastDir := filePath
    _, err = os.Stat(dir)
    for dir != "." && dir != "" && os.IsNotExist(err) {
        lastDir = dir
        dir = filepath.Dir(dir)
        _, err = os.Stat(dir)
    }

    return true, lastDir, err
}

/*
 * ToDo: support HDFS in the future
 */
func FSUtilsStatFile(filePath string) (os.FileInfo, error) {
    if FSUtilsIsHDFSPath(filePath) {
        return hdfsutils.Stat(filePath)
    }
    return os.Stat(filePath)
}

func FSUtilsDebugDirAndFileStatInfo(path string) error {
    fileInfo, errNo := os.Stat(path)
    if errNo != nil {
        StorageLogger.Errorf("Stat path dirPath %s failed in func FSUtilsDeleteDir, err: %s\n", path, errNo.Error())
        return errNo
    }
    fileSys, _ := fileInfo.Sys().(*syscall.Stat_t)
    StorageLogger.Debugf("In FSUtilsDeleteDir func Nlink:%d Ino:%d Size:%d X__pad0:%d Mode:%d",
        fileSys.Nlink, fileSys.Ino, fileSys.Size, fileSys.X__pad0, fileSys.Mode)
    accessTime := time.Unix(fileSys.Atim.Sec, fileSys.Atim.Nsec)
    StorageLogger.Debugf("Dir %s access time: %v\n",
        path, accessTime)
    modifyTime := time.Unix(fileSys.Mtim.Sec, fileSys.Mtim.Nsec)
    StorageLogger.Debugf("Dir %s modify time: %v\n",
        path, modifyTime)
    changeTime := time.Unix(fileSys.Ctim.Sec, fileSys.Ctim.Nsec)
    StorageLogger.Debugf("Dir %s change time: %v\n",
        path, changeTime)
    return nil
}

func FSUtilsIsOSPrefix(key string) (bool, string) {
    if strings.HasSuffix(key, "*") {
        return true, strings.TrimSuffix(key, "*")
    } else {
        return false, ""
    }
}

//Generate full path 'os@cluster:bucket/bio-object' from 'os@cluster:bucket/bio-*', 'bio-', 'object'
func FSUtilsBuildOSAddressWithPrefix(address, prefix, object string) string {
    return strings.TrimSuffix(address, prefix + "*") + object
}

func FSUtilsIsOSPath(path string) bool {
    err, cluster, _, _ := FSUtilsParseFileURI(path)
    if err != nil {
        return false
    }

    manifest := xstorage.GetMountMgr().GetStorageManifest(cluster)
    if manifest == nil {
        return false
    }

    switch manifest.FSType {
    case xconfig.OSS_ALIBABA, xconfig.OSS_TENCENT, xconfig.OSS_RGW:
        return true
    default:
        return false
    }
}

//Convert object name from 'path/to/file' to 'path-to-file'
func FSUtilsGenerateOSIdFromPath(path string) string{
    return strings.Replace(path, "/", "-", -1)
}

//Get bucket and object from OS address like 'os@cluster:bucket/object' or 'os@cluster:bucket'
func FSUtilsParseOSAddress(address string) (error, string, string) {
    err, _, _, path := FSUtilsParseFileURI(address)
    if err != nil {
        return err, "", ""
    }

    err = fmt.Errorf("Invalid os address format %s", address)
    strItems := strings.Split(path, "/")
    if len(strItems) < 1 {
        return err, "", ""
    }
    bucket := strItems[0]
    if len(strItems) == 1 {
        return nil, bucket, ""
    }
    obj := strings.TrimPrefix(path, bucket + "/")
    for {
        if strings.HasPrefix(obj, "/") {
            obj = strings.TrimPrefix(obj, "/")
        } else {
            break
        }
    }
    return nil, bucket, obj
}

//Get all files under the directory recursively.
//Return file paths with directory prefix like 'dir/file'.
func FSUtilsReadDirFilesRecursively(dirPath string) (error, []string) {
    for {
        if strings.HasSuffix(dirPath, "/") {
            dirPath = strings.TrimSuffix(dirPath, "/")
        } else {
            break
        }
    }

    return ReadDirFilesRecursively("", dirPath)
}

func ReadDirFilesRecursively(prefix, dirPath string) (error, []string) {
    var files []string
    var preTime time.Time
    perfStats.StartProfileReadDir(&preTime)
    dirItems, err := FSUtilsReadDirItems(dirPath)
    perfStats.EndProfileReadDir(preTime)
    if err != nil {
        StorageLogger.Errorf("Read directory %s error %v\n", dirPath, err)
        GetDashBoard().UpdateStats(STAT_OP_READDIR_FAIL, 1)
        return err, nil
    }
    for _, file := range dirItems {
        if !file.IsDir() {
            files = append(files, prefix + file.Name())
        }else {
            newPrefix := fmt.Sprintf("%s%s/", prefix, file.Name())
            newDirPath := fmt.Sprintf("%s/%s", dirPath, file.Name())
            err, subFiles := ReadDirFilesRecursively(newPrefix, newDirPath)
            if err != nil {
                return err, nil
            }
            files = append(files, subFiles...)
        }
    }
    return nil, files
}

func FSUtilsBuildFsPath(dir, name string) string {
    for {
        if strings.HasSuffix(dir, "/") {
            dir = strings.TrimSuffix(dir, "/")
        } else {
            break
        }
    }

    return fmt.Sprintf("%s/%s", dir, name)
}

type WalkFunc func(path string, info os.FileInfo, err error) error
var SkipDir = errors.New("skip this directory")

func WalkPath(path string, walkFunc WalkFunc) error {
    path = strings.TrimSuffix(path, "/")
    info, err := os.Lstat(path)
    if err != nil {
        err = walkFunc(path, nil, err)
    } else {
        err = walk(path, info, walkFunc)
    }
    if err == SkipDir {
        return nil
    }
    return err
}

func walk(path string, info os.FileInfo, walkFn WalkFunc) error {
    if !info.IsDir() {
        return walkFn(path, info, nil)
    } else {
        items, err := FSUtilsReadDirItems(path)
        if err != nil {
            return walkFn(path, info, err)
        }

        for _, item := range items {
            name := path + "/" + item.Name()
            if err := walk(name, item, walkFn); err != nil && err != SkipDir {
                return err
            }
        }

        err = walkFn(path, info, nil)
        if err != nil && err != SkipDir {
            return err
        }
        return nil
    }
}

func FSUtilsDeleteFilesByPatterns(workDir string, patterns []string) error {
    walkFunc := func(path string, info os.FileInfo, err error) error{
        if info.IsDir() {
            return nil
        }
        match := false
        for _, pattern := range patterns {
            if match = blparser.BLUtilsMatchStringByPattern(path, pattern); match {
                break
            }
        }
        if match {
            return FSUtilsDeleteFile(path)
        }
        return nil
    }

    return WalkPath(workDir, walkFunc)
}

