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
package hdfsutils

import (
	"os"
    "errors"
    "path"
    "path/filepath"
    "io"
    "fmt"
    "io/ioutil"

	"github.com/colinmarc/hdfs"
    . "github.com/xtao/bioflow/common"
)

func Chmod(path string, mode uint, recursive bool) error {
	expanded, client, err := getClientAndExpandedPath(path)
	if err != nil {
		return err
	}

    status := 0
	visit := func(p string, fi os.FileInfo) {
		err := client.Chmod(p, os.FileMode(mode))

		if err != nil {
            StorageLogger.Errorf("HDFS chmod file %s error: %s\n",
                p, err.Error())
            status = 1
		}
	}

    p := expanded
    if p != "" {
		if recursive {
			err = walk(client, p, visit)
			if err != nil {
                return err
			}
		} else {
			info, err := client.Stat(p)
			if err != nil {
                return err
			}

			visit(p, info)
            if status == 1 {
                return errors.New("Chmod file error")
            }
		}
	}

    return nil
}

func Chown(path string, owner string, group string, recursive bool) error {
	expanded, client, err := getClientAndExpandedPath(path)
	if err != nil {
        StorageLogger.Errorf("HDFS utils fail get expand path for %s: %s\n",
            path, err.Error())
        return err
	}

    status := 0
	visit := func(p string, fi os.FileInfo) {
		err := client.Chown(p, owner, group)
		if err != nil {
            StorageLogger.Errorf("HDFS fail to chown file %s: %s\n",
                p, err.Error())
			status = 1
		}
	}

    p := expanded
    if p != "" {
		if recursive {
			err = walk(client, p, visit)
			if err != nil {
                return err
			}
		} else {
			info, err := client.Stat(p)
			if err != nil {
                return err
			}

			visit(p, info)
            if status == 1 {
                return errors.New("Fail to chown file")
            }
		}
	}

    return nil
}

func MkDir(path string, all bool) error {
    var paths []string
    paths = append(paths, path)
    return MkDirs(paths, all)
}

func MkDirs(paths []string, all bool) error {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
        StorageLogger.Debugf("Fail to normalize paths %v: %s\n",
            paths, err.Error())
        return err
	}

	client, err := getClient(nn)
	if err != nil {
        return err
	}

	for _, p := range paths {
		if hasGlob(p) {
			return &os.PathError{"mkdir", p, os.ErrNotExist}
		}

		var mode = 0755 | os.ModeDir
		if all {
			err = client.MkdirAll(p, mode)
		} else {
			err = client.Mkdir(p, mode)
		}

		if err != nil && !(all && os.IsExist(err)) {
            return err
		}
	}

    return nil
}

func Rename(srcPath string, dstPath string) error {
    var paths []string
    paths = append(paths, srcPath)
    paths = append(paths, dstPath)

    return MoveFiles(paths, false, false)
}

func MoveFiles(paths []string, force, treatDestAsFile bool) error {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
        return err
	}

	if len(paths) < 2 {
        return errors.New("move require at least 2 paths")
	} else if hasGlob(paths[len(paths)-1]) {
		return errors.New("The destination must be a single path.")
	}

	client, err := getClient(nn)
	if err != nil {
        return err
	}

	dest := paths[len(paths)-1]
	sources, err := expandPaths(client, paths[:len(paths)-1])
	if err != nil {
        return err
	}

	destInfo, err := client.Stat(dest)
	if err != nil && !os.IsNotExist(err) {
        return err
	}

	exists := !os.IsNotExist(err)
	if exists && !treatDestAsFile && destInfo.IsDir() {
		return moveInto(client, sources, dest, force)
	} else {
		if len(sources) > 1 {
			return errors.New("Can't move multiple sources into the same place.")
		}

		return moveTo(client, sources[0], dest, force)
	}
}

func moveInto(client *hdfs.Client, sources []string, dest string, force bool) error{
	for _, source := range sources {
		_, name := path.Split(source)

		fullDest := path.Join(dest, name)
		err := moveTo(client, source, fullDest, force)
        if err != nil {
            return err
        }
	}

    return nil
}

func moveTo(client *hdfs.Client, source, dest string, force bool) error{
	sourceInfo, err := client.Stat(source)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			pathErr.Op = "rename"
		}
        return err
	}

	destInfo, err := client.Stat(dest)
	if err == nil {
		if destInfo.IsDir() && !sourceInfo.IsDir() {
			return errors.New("Can't replace directory with non-directory.")
		} else if !force {
			return &os.PathError{"rename", dest, os.ErrExist}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	err = client.Rename(source, dest)
	if err != nil {
		return err
	}

    return nil
}

func RmFile(path string, recursive bool, force bool) error {
    var paths []string
    paths = append(paths, path)
    return RmFiles(paths, recursive, force)
}

func RmFiles(paths []string, recursive bool, force bool) error{
	expanded, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
        return err
	}

	for _, p := range expanded {
		info, err := client.Stat(p)
		if err != nil {
			if force && os.IsNotExist(err) {
				continue
			}

			if pathErr, ok := err.(*os.PathError); ok {
				pathErr.Op = "remove"
			}

			continue
		}

		if !recursive && info.IsDir() {
			return &os.PathError{"remove", p, errors.New("file is a directory")}
		}

		err = client.Remove(p)
        if err != nil {
            return err
        }
	}

    return nil
}

func ReadDir(dirPath string)([]os.FileInfo, error) {
	path, client, err := getClientAndExpandedPath(dirPath)
	if err != nil {
        return nil, err
	}

	fi, err := client.Stat(path)
	if err != nil {
        return nil, err
	}

	if !fi.IsDir() {
	    return nil, errors.New("Can't readdir a file")
	}

	return readDir(client, path)
}

func readDir(client *hdfs.Client, dir string) ([]os.FileInfo, error) {
	dirReader, err := client.Open(dir)
	if err != nil {
        return nil, err
	}
    defer dirReader.Close()


    retFiles := make([]os.FileInfo, 0)
	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
            return nil, err
		}
        if partial != nil {
            retFiles = append(retFiles, partial ...)
        }
	}

    return retFiles, nil
}

func PutFile(destFile string, data []byte) error {
	dest, nn, err := normalizePath(destFile)
	if err != nil {
		return err
	}


	client, err := getClient(nn)
	if err != nil {
        return err
	}

	// If the destination is an existing directory, place it inside. Otherwise,
	// the destination is really the parent directory, and we need to rename the
	// source directory as we copy.
	existing, err := client.Stat(dest)
	if err == nil {
		if existing.IsDir() {
            return errors.New("Can't write data to a directory")
		} else {
		}
	} else if !os.IsNotExist(err) {
        return err
	}

	writer, err := client.Create(dest)
	if err != nil {
	    return err
	}

	defer writer.Close()
	_, err = io.WriteString(writer, string(data))
	if err != nil {
        return err
	}

	return nil
}

func ReadFile(path string) ([]byte, error) {
	p, client, err := getClientAndExpandedPath(path)
	if err != nil {
        return nil, err
	}

	file, err := client.Open(p)
	if err != nil || file.Stat().IsDir() {
        return nil, err
	}
    defer file.Close()

    fi := file.Stat()
    if fi.IsDir() {
        err = errors.New("Can't read a directory")
        return nil, err
    }


	reader := io.NewSectionReader(file, 0, fi.Size())

	return ioutil.ReadAll(reader)
}


/* It is a very expensive operation, each stat is a remote
 * operation. The caller should note that and minimize its
 * usage. We need seek better approach.
 */
func GetShortestNonExistPath(path string) (bool, string, error) {
    p, client, err := getClientAndExpandedPath(path)
    if err != nil {
        return true, "", err
    }

    /*The path p is now a normal relative filepath*/
    dir := filepath.Dir(p)
    if dir == "." {
        return true, "", errors.New("Empty path")
    }
    lastDir := p
    _, err = client.Stat(dir)
    if err == nil {
        return false, "", nil
    }

    for dir != "." && dir != "" && os.IsNotExist(err) {
        lastDir = dir
        dir = filepath.Dir(dir)
        _, err = client.Stat(dir)
    }
    
    /*should return the normalized HDFS path*/
    nameNode, _ := getNameNode(path)
    foundDir := fmt.Sprintf("hdfs://%s/%s", nameNode, lastDir)
    return true, foundDir, err
}

/*
 * Check whether a HDFS file exists or not
 *
 */
func Stat(path string) (os.FileInfo, error) {
    p, client, err := getClientAndExpandedPath(path)
    if err != nil {
        return nil, err
    }

    return client.Stat(p)
}
