/* 
 Copyright (c) 2019 XTAO technology <www.xtaotech.com>
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
)

const (
    IO_LABEL_CACHE_DIRS = "XT_IOL_CACHE_DIRS"
    IO_LABEL_CACHE_FILES = "XT_IOL_CACHE_FILES"
    IO_LABEL_CACHE_EPH_LEVEL = "XT_IOL_CACHE_EPH_LEVEL"
    IO_LABEL_CACHE_EPH_PATTERN = "XT_IOL_CACHE_EPH_PATTERN"
    IO_LABEL_CACHE_EPH_MAP = "XT_IOL_CACHE_EPH_MAP"
)

type IOLabelFactory struct {
    cacheDirs map[string]string
    cacheFiles map[string]string
    ephMap map[string]string
}

func NewIOLabelFactory() *IOLabelFactory {
    return &IOLabelFactory {
        cacheDirs: make(map[string]string),
        cacheFiles: make(map[string]string),
        ephMap: make(map[string]string),
    }
}

func (factory *IOLabelFactory)CacheDir(dir string, level IsoLevel) {
    factory.cacheDirs[dir] = level.String()
}

func (factory *IOLabelFactory)CacheFile(file string, level IsoLevel) {
    factory.cacheFiles[file] = level.String()
}

func (factory *IOLabelFactory)MarkEpheremal(path string, level EphLevel) {
    factory.ephMap[path] = level.String()
}

/*
 * The IO cache hints will be passed to server end via the task labels.
 * the format is as follows:
 *   path:level|path:level|...
 */
func (factory *IOLabelFactory)ToLabels() map[string]string {
    labels := make(map[string]string)
    cacheDirs := ""
    for key, value := range factory.cacheDirs {
        if cacheDirs != "" {
            cacheDirs += "|"
        }
        cacheDirs += key + ":" + value
    }
    labels[IO_LABEL_CACHE_DIRS] = cacheDirs

    cacheFiles := ""
    for key, value := range factory.cacheFiles {
        if cacheFiles != "" {
            cacheFiles += "|"
        }
        cacheFiles += key + ":" + value
    }
    labels[IO_LABEL_CACHE_FILES] = cacheFiles

    ephPatterns := ""
    for key, value := range factory.ephMap {
        if ephPatterns != "" {
            ephPatterns += "|"
        }
        ephPatterns += key + ":" + value
    }
    labels[IO_LABEL_CACHE_EPH_MAP] = ephPatterns

    return labels
}
