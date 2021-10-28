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

type CacheScheduler struct {
    targetDirs []string
    targetFiles []string
    ioAttr *IOAttr
}

func NewCacheScheduler(dirs []string, files []string, ioAttr *IOAttr) *CacheScheduler {
    return &CacheScheduler{
        targetDirs: dirs,
        targetFiles: files,
        ioAttr: ioAttr,
    }
}

func (scheduler *CacheScheduler)GenerateIOLabels() map[string]string {
    if scheduler.ioAttr == nil {
        return nil
    }

    ioAttr := scheduler.ioAttr
    doCache := false
    labelFactory := NewIOLabelFactory()

    /*
     * Case 1: cache dirs or files when rewrite and io is random
     */
    if (ioAttr.ReWrite() || ioAttr.Write() || ioAttr.ReRead()) && (ioAttr.IORandom() || ioAttr.IOHybrid()) {
        doCache = true
    }
    if doCache {
        for _, dirPath := range scheduler.targetDirs {
            labelFactory.CacheDir(dirPath, ioAttr.IsoLevel())
        }
        for _, filePath := range scheduler.targetFiles {
            labelFactory.CacheFile(filePath, ioAttr.IsoLevel())
        }
    }

    /*
     * Case 2: mark epheremal file patterns if have
     */
    if ioAttr.EphPattern() != "" {
        labelFactory.MarkEpheremal(ioAttr.EphPattern(),
            ioAttr.EphLevel())
    }
    for pattern, level := range ioAttr.EphMap() {
        labelFactory.MarkEpheremal(pattern, level)
    }

    return labelFactory.ToLabels()
}
