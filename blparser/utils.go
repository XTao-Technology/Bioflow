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
    "regexp"
    )

func BlUtilsParseSuffix(cmd string, sepStr string) string {
    cmdItems := strings.Split(cmd, sepStr)
    if len(cmdItems) != 2 {
        return ""
    } else {
        return cmdItems[1]
    }
}

func BlUtilsParseDotSuffix(cmd string) string {
    return BlUtilsParseSuffix(cmd, ".")
}

func BlUtilsParseDollarSuffix(cmd string) string {
    return BlUtilsParseSuffix(cmd, "$")
}

func BLUtilsIdentifyTokenAndPrefix(word string, token string) (bool, string, string) {
    tokenIndex := strings.Index(word, token)
    if tokenIndex == -1 {
        return false, word, ""
    } else {
		rs := []rune(word)
		return true, string(rs[0:tokenIndex]),
            string(rs[tokenIndex + 1:len(word)])
    }
}

func BLUtilsCheckSplitToken(word string, token string) (bool, []string) {
    if strings.Index(word, token) == -1 {
        return false, nil
    } else {
        return true, strings.Split(word, token)
    }
}

/*match string by a pattern, it is not regular expression, but defined by bioflow*/
func BLUtilsMatchStringByPattern(candidate string,
    pattern string) bool {
	return _BLUtilsMatchStringByPattern(candidate, pattern, false)
}

func _BLUtilsMatchStringByPattern(candidate string,
    pattern string, strict bool) bool {
    /*
	 *	the pattern looks like *str1*str2*str3* and so on
	 *  the * can match any number or character.
	 *
	 */
    matchedStr := candidate
    strictMatch := true
    patternItems := strings.Split(pattern, "*")
    for j := len(patternItems) - 1; j >= 0; j -- {
        if j != len(patternItems) - 1 {
            strictMatch = false
        }
        subPattern := patternItems[j]
        if subPattern == "" {
            continue
        }

        if strictMatch {
            if strings.HasSuffix(matchedStr, subPattern) {
                lastIndex := strings.LastIndex(matchedStr,
                    subPattern)
                rs := []rune(matchedStr)
                matchedStr = string(rs[0:lastIndex])
            } else {
                return false
            }
        } else {
            lastIndex := strings.LastIndex(matchedStr, subPattern)
            if lastIndex == -1 {
                return false
            }

            rs := []rune(matchedStr)
            matchedStr = string(rs[0:lastIndex])
        }
    }

    if patternItems[0] != "" && strict {
        if strings.HasPrefix(candidate, patternItems[0]) {
            return true
        } else {
            return false
        }
    }

    return true
}

func BLUtilsMatchStringByPatterns(candidate string, patterns []string) bool {
    return _BLUtilsMatchStringByPatterns(candidate, patterns, false)
}

func BLUtilsMatchStringByPatternsStrict(candidate string, patterns []string) bool {
    return _BLUtilsMatchStringByPatterns(candidate, patterns, true)
}

func _BLUtilsMatchStringByPatterns(candidate string,
    patterns []string, strict bool) bool {
    for i := 0; i < len(patterns); i ++ {
        if patterns[i] != "" {
            if _BLUtilsMatchStringByPattern(candidate,
                    patterns[i], strict) {
                return true
            }
        }
    }

    return false
}

/*
 * Match string in Bpipe style, the string looks like
 *   1)str1*str2*str3: the * can match any number of any characters
 *
 * it should return matched substring
 */
func BLUtilsGroupSubStringByBpipePattern(candidate string,
    pattern string) string {
    /*
     * the pattern looks like str1*str2*str3 and so on
     * the * can match any number or character.
     *
     */
    if pattern == "" {
        return ""
    }
    matchedStr := candidate
    patternItems := strings.Split(pattern, "*")
    startIndex := -1
    endIndex := 0
    for j := 0; j < len(patternItems); j ++ {
        subPattern := patternItems[j]
        if subPattern == "" {
            if j == len(patternItems) - 1 {
                endIndex += len(matchedStr)
            }
            continue
        }

        index := strings.Index(matchedStr, subPattern)
        if index == -1 {
            return ""
        }
        if startIndex == -1 {
            startIndex = index
        }
        endIndex += index + len(subPattern)

        rs := []rune(matchedStr)
        matchedStr = string(rs[index + len(subPattern):len(matchedStr)])
    }

    rs := []rune(candidate)
    /*
     * The startindex is -1 only if the full match is available.
     */
    if startIndex == -1 {
        startIndex = 0
    }
    return string(rs[startIndex:endIndex])
}

/*
 * Match number in Bpipe style, the string looks like
 *   1)str1%str2: the % can match any number of digits,
 *   2)str1%
 *
 * It should return matched number
 */
func BLUtilsFindDigitByBpipePattern(candidate string,
    pattern string) string {
	/*
	 *	the pattern looks like str1%str2 and so on
	 *  the % can match any number or character.
	 *
	 */
	matchedStr := candidate
	patternItems := strings.Split(pattern, "%")	
    prefixPattern := patternItems[0]
    secondPattern := ""
    if len(patternItems) > 1 {
        secondPattern = patternItems[1]
    }
    expr := prefixPattern + "([0-9]+)" + secondPattern
    reg := regexp.MustCompile(expr)
    matchedStr = reg.FindString(candidate)
    if matchedStr == "" {
        return matchedStr
    }
    noPrefix := strings.TrimPrefix(matchedStr, prefixPattern)
    digit := strings.TrimSuffix(noPrefix, secondPattern)
	return digit
}

func BLUtilsMatchSortPattern(candidate string, pattern string) string {
    /* If the pattern has %, match by digit pattern, otherwise match
     * by normal bpipe behaviour
     */
    if strings.Index(pattern, "%") >= 0 {
        return BLUtilsFindDigitByBpipePattern(candidate, pattern)
    } else {
        return BLUtilsGroupSubStringByBpipePattern(candidate, pattern)
    }
}
