/* 
 Copyright (c) 2018 XTAO technology <www.xtaotech.com>
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

package common

import (
    "archive/zip"
    "io"
    "os"
    "path/filepath"
    "strings"
)

type ZipWriter struct {
    zipDir string
    zipFile string
    includeDir bool
}

func (writer *ZipWriter)Write() error {
    zipfile, err := os.Create(writer.zipFile)
    if err != nil {
        return err
    }
    defer zipfile.Close()

    archive := zip.NewWriter(zipfile)
    defer archive.Close()

    info, err := os.Stat(writer.zipDir)
    if err != nil {
        return nil
    }

    var baseDir string
    if writer.includeDir {
        if info.IsDir() {
            baseDir = filepath.Base(writer.zipDir)
        }
    }

    filepath.Walk(writer.zipDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        header, err := zip.FileInfoHeader(info)
        if err != nil {
            return err
        }

        if baseDir != "" {
            header.Name = filepath.Join(baseDir,
                strings.TrimPrefix(path, writer.zipDir))
        }
        if !writer.includeDir {
            if strings.TrimPrefix(path, writer.zipDir) == "" {
                return nil
            }
        }

        if info.IsDir() {
            header.Name += "/"
        } else {
            header.Method = zip.Deflate
        }

        writer, err := archive.CreateHeader(header)
        if err != nil {
            return err
        }

        if info.IsDir() {
            return nil
        }

        file, err := os.Open(path)
        if err != nil {
            return err
        }
        defer file.Close()
        _, err = io.Copy(writer, file)
        return err
    })

    return err
}

func NewZipWriter(dir string, zipFile string, includeDir bool) *ZipWriter {
    return &ZipWriter{zipDir: dir, zipFile: zipFile, includeDir: includeDir}
}

type ZipExtractor struct {
    zipFile string
    extractDir string
}

func (extractor *ZipExtractor)Extract() error {
    reader, err := zip.OpenReader(extractor.zipFile)
    if err != nil {
        return err
    }

    if err := os.MkdirAll(extractor.extractDir, 0755); err != nil {
        return err
    }

    for index, file := range reader.File {
        fileName := strings.TrimPrefix(file.Name, reader.File[0].Name)
        path := filepath.Join(extractor.extractDir, fileName)
        if file.FileInfo().IsDir() {
            if index != 0 {
                os.MkdirAll(path, file.Mode())
            }
            continue
        }

        fileReader, err := file.Open()
        if err != nil {
            return err
        }
        defer fileReader.Close()

        targetFile, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, file.Mode())
        if err != nil {
            return err
        }
        defer targetFile.Close()

        if _, err := io.Copy(targetFile, fileReader); err != nil {
            return err
        }
    }

    return nil
}

func NewZipExtractor(dir string, zipFile string) *ZipExtractor {
    return &ZipExtractor{extractDir: dir, zipFile: zipFile}
}
