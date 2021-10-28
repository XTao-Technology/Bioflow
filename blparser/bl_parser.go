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

package blparser

import (
    "strings"
    "sort"

    . "github.com/xtao/bioflow/message"
    )

type blParser struct {
}

func NewBLParser() *blParser {
    return &blParser {
    }
}


/*
 try to identify the following pattern:
 1) $files.*
 2) $inputs.xx or $inputs.xx.SplitJoin('XYZ ')
 3) $input.xx
 4) $output.xx
 5) $branch.xx
 6) $variable.xx
 7) $sys.dir

*/

func (parser *blParser) IdentifyPlaceHolder(token string) (string, string) {
    /*
     * the placeholders should be specified in two ways:
     * 1) $variable: return variable string with empty suffix
     * 2) ${variable}X: return variable with suffix X
     */
    if strings.HasPrefix(token, "{") {
        start := 1
        end := strings.Index(token, "}")
		rs := []rune(token)
		return string(rs[start:end]),
            string(rs[end + 1:len(token)])
    } else {
        return token, ""
    }
}

func (parser *blParser) TranslatePlaceHolders(tokenString string,
    translatorFn PlaceHolderTranslator) *BuildStageErrorInfo {
    lphParser := NewLPHParser()
    return lphParser.TranslatePlaceHolders(tokenString,
        translatorFn)
}

func (parser *blParser) ShardFileListByGroupSize(files []string,
    groupSize int) [][]string {
    if groupSize == 0 || len(files) % groupSize != 0 {
        return nil
    }

    groupList := make([][]string, 0)
    var fileGroup []string = nil
    for i := 0; i < len(files); i ++ {
        if (i % groupSize) == 0 {
            if fileGroup != nil {
                groupList = append(groupList, fileGroup)
            }
            fileGroup = make([]string, 0)
        }
        fileGroup = append(fileGroup, files[i])

        if i == len(files) - 1 {
            if (i + 1) % groupSize == 0 {
                groupList = append(groupList, fileGroup)
            }
        }
    }

    return groupList
}

/*
 * called by shardfiles pipeline item to match and group input files:
 * 1) first match files with pattern like "pattern1;pattern2"
 * 2) group files via a regular expression, the files matched by 
 *    pattern by same sub string will be in same group
 */
func (parser *blParser) ShardFileListByPattern(files []string, groupPattern string, 
    matchPattern string, sortPattern string) [][]string {
    doGroup := false
    doMatch := false
    if groupPattern != "" {
        doGroup = true
    }
    matchPatterns := make([]string, 0)
    if matchPattern != "" {
        doMatch = true
        /*
         * The match pattern can be more than one separated by
         * ;
         */
        matchPatterns = strings.Split(matchPattern, ";")
    }

    /*
     * Becuse recover a failed job need rebuild the flow graph. So we need
     * a deterministic approach to generate stages from the input data. So
     * all the input files need be sorted first and then processed. This
     * will guanrantee generate same file group list. 
     */
    sort.Strings(files)

    fileGroup := make(map[string][]string)
    for i := 0; i < len(files); i ++ {
        matchStr := ""
        if doMatch {
			if BLUtilsMatchStringByPatterns(files[i], matchPatterns) {
				matchStr = files[i]
			}
        }
        if (matchStr != "" && doGroup) || !doMatch {
            matchStr = BLUtilsGroupSubStringByBpipePattern(files[i],
                groupPattern)
        }
        if matchStr != "" {
            if strList, ok := fileGroup[matchStr]; ok {
                strList = append(strList, files[i])
                fileGroup[matchStr] = strList
            } else {
                strList := make([]string, 0)
                strList = append(strList, files[i])
                fileGroup[matchStr] = strList
            }
        }
    }

    /*
     * We need return sorted file group list to ensure
     * determinism
     */
    groupKeys := make([]string, 0)
    groupList := make([][]string, 0)

    for key, _ := range fileGroup {
        groupKeys = append(groupKeys, key)
    }
    sort.Strings(groupKeys)
    for i := 0; i < len(groupKeys); i ++ {
        groupList = append(groupList, fileGroup[groupKeys[i]])
    }

    /*try to sort each file group*/
    if sortPattern != "" {
        sortedGroupList := make([][]string, 0)
        for i := 0; i < len(groupList); i ++ {
            subList := parser.SortFileListByPattern(groupList[i], sortPattern)
            sortedGroupList = append(sortedGroupList, subList)
        }

        return sortedGroupList
    }

    return groupList
}

func (parser *blParser)SortFileListByPattern(files []string, pattern string) []string {
    filesMap := make(map[string][]string)
    keys := make([]string, 0)
    unMatchList := make([]string, 0)
    for i := 0; i < len(files); i ++ {
        key := BLUtilsMatchSortPattern(files[i], pattern)
        if key == "" {
            unMatchList = append(unMatchList, files[i])
        } else {
            keys = append(keys, key)
            if subList, ok := filesMap[key]; ok && subList != nil {
                subList = append(subList, files[i])
                filesMap[key] = subList
            } else {
                subList := make([]string, 0)
                subList = append(subList, files[i])
                filesMap[key] = subList
            }
        }
    }
    sort.Strings(keys)
    sortedList := make([]string, 0)
    for i := 0; i < len(keys); i ++ {
        subList := filesMap[keys[i]]
        if subList != nil {
            sortedList = append(sortedList, subList ...)
        }
    }
    sortedList = append(sortedList, unMatchList ...)

    return sortedList
}

/*identify array of patterns separated by ;*/
func (parser *blParser)IdentifySeparatePatterns(pattern string) []string {
    patternItems := strings.Split(pattern, ";")
    patterns := make([]string, 0)
    for i := 0; i < len(patternItems); i ++ {
        if patternItems[i] != "" {
            patterns = append(patterns, patternItems[i])
        }
    }

    return patterns
}
