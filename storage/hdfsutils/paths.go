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
	"errors"
	"io"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/colinmarc/hdfs"
	. "github.com/xtao/bioflow/common"
)

var (
	errMultipleNamenodeUrls = errors.New("Multiple namenode URLs specified")
	rootPath                = userDir()
)

func userDir() string {
	currentUser, err := hdfs.Username()
	if err != nil || currentUser == "" {
		return "/"
	}

	return path.Join("/user", currentUser)
}

func normalizePath(path string) (string, string, error) {
    var paths []string
    paths = append(paths, path)
    retPaths, nm, err := normalizePaths(paths)
    if err != nil {
        return "", "", err
    } else {
        return retPaths[0], nm, nil
    }
}

// normalizePaths parses the hosts out of HDFS URLs, and turns relative paths
// into absolute ones (by appending /user/<user>). If multiple HDFS urls with
// differing hosts are passed in, it returns an error.
func normalizePaths(paths []string) ([]string, string, error) {
	namenode := ""
	cleanPaths := make([]string, 0)

	for _, rawurl := range paths {
		url, err := url.Parse(rawurl)
		if err != nil {
            StorageLogger.Errorf("Fail to parse hdfs url for %s: %s\n",
                rawurl, err.Error())
			return nil, "", err
		}

		if url.Host != "" {
			if namenode != "" && namenode != url.Host {
				return nil, "", errMultipleNamenodeUrls
			}

			namenode = url.Host
		}

		p := path.Clean(url.Path)
		if !path.IsAbs(url.Path) {
			p = path.Join(rootPath, p)
		}

		cleanPaths = append(cleanPaths, p)
	}

	return cleanPaths, namenode, nil
}

func getNameNode(path string) (string, error) {
    url, err := url.Parse(path)
    if err != nil {
        return "", err
    }

    return url.Host, nil
}

func getClientAndExpandedPath(path string) (string, *hdfs.Client, error) {
    var paths []string
    paths = append(paths, path)
    retPaths, client, err := getClientAndExpandedPaths(paths)
    if err != nil {
        return "", nil, err
    } else {
        return retPaths[0], client, nil
    }
}

func getClientAndExpandedPaths(paths []string) ([]string, *hdfs.Client, error) {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
		return nil, nil, err
	}

	client, err := getClient(nn)
	if err != nil {
		return nil, nil, err
	}

	expanded, err := expandPaths(client, paths)
	if err != nil {
		return nil, nil, err
	}

	return expanded, client, nil
}

// TODO: not really sure checking for a leading \ is the way to test for
// escapedness.
func hasGlob(fragment string) bool {
	match, _ := regexp.MatchString(`([^\\]|^)[[*?]`, fragment)
	return match
}

// expandGlobs recursively expands globs in a filepath. It assumes the paths
// are already cleaned and normalize (ie, absolute).
func expandGlobs(client *hdfs.Client, globbedPath string) ([]string, error) {
	parts := strings.Split(globbedPath, "/")[1:]
	var res []string
	var splitAt int

	for splitAt = range parts {
		if hasGlob(parts[splitAt]) {
			break
		}
	}

	var base, glob, next, remainder string
	base = "/" + path.Join(parts[:splitAt]...)
	glob = parts[splitAt]

	if len(parts) > splitAt+1 {
		next = parts[splitAt+1]
		remainder = path.Join(parts[splitAt+2:]...)
	} else {
		next = ""
		remainder = ""
	}

	list, err := client.ReadDir(base)
	if err != nil {
		return nil, err
	}

	for _, fi := range list {
		match, _ := path.Match(glob, fi.Name())
		if !match {
			continue
		}

		if !hasGlob(next) {
			_, err := client.Stat(path.Join(base, fi.Name(), next))
			if err != nil && !os.IsNotExist(err) {
				return nil, err
			} else if os.IsNotExist(err) {
				continue
			}
		}

		newPath := path.Join(base, fi.Name(), next, remainder)
		if hasGlob(newPath) {
			children, err := expandGlobs(client, newPath)
			if err != nil {
				return nil, err
			}

			res = append(res, children...)
		} else {
			res = append(res, newPath)
		}
	}

	return res, nil
}

func expandPaths(client *hdfs.Client, paths []string) ([]string, error) {
	var res []string

	for _, p := range paths {
		if hasGlob(p) {
			expanded, err := expandGlobs(client, p)
			if err != nil {
				return nil, err
			}

			res = append(res, expanded...)
		} else {
			res = append(res, p)
		}
	}

	return res, nil
}

type walkFunc func(string, os.FileInfo)

func walk(client *hdfs.Client, root string, visit walkFunc) error {
	rootInfo, err := client.Stat(root)
	if err != nil {
		return err
	}

	visit(root, rootInfo)
	if rootInfo.IsDir() {
		err = walkDir(client, root, visit)
		if err != nil {
			return err
		}
	}

	return nil
}

func walkDir(client *hdfs.Client, dir string, visit walkFunc) error {
	dirReader, err := client.Open(dir)
	if err != nil {
		return err
	}

	var partial []os.FileInfo
	for ; err != io.EOF; partial, err = dirReader.Readdir(100) {
		if err != nil {
			return err
		}

		for _, child := range partial {
			childPath := path.Join(dir, child.Name())
			visit(childPath, child)

			if child.IsDir() {
				err = walkDir(client, childPath, visit)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
