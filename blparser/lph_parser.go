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
    "errors"
    "fmt"

    . "github.com/xtao/bioflow/common"
    . "github.com/xtao/bioflow/message"
)

/*
 * Parser to parse strings of place holders in lex way.
 */
type lphParser struct {
    curPrefix string

    /*
     *sometimes we need look next token, but don't use
     *it, then we need push back it for others to parse.
     *the pushed back token is stored in this field.
     */
    nextToken *Token

    /*scanner to get next token from stream*/
    scanner *Scanner

    /*result user for Translate PlaceHolders result*/
    result *BuildStageErrorInfo

    /*The token is currently being parsed*/
    curTokenValue string
}

func NewLPHParser() *lphParser {
    return &lphParser{
            curPrefix: "",
            nextToken: nil,
            scanner: nil,
            result: nil,
            curTokenValue: "",
        }
}

func (parser *lphParser) NextToken() *Token {
    tok := parser.nextToken
    parser.nextToken = nil
    if tok == nil && parser.scanner != nil {
        tok = parser.scanner.Scan()
    }

    return tok
}

func (parser *lphParser) PushBackNextToken(tok *Token) {
    parser.nextToken = tok
}

func (parser *lphParser) SetScanner(scanner *Scanner) {
    parser.scanner = scanner
}

func (parser *lphParser) SetCurTokenValue(value string) {
    parser.curTokenValue += value
}

func(parser *lphParser) CleanupCurTokenValue() {
    parser.curTokenValue = ""
}

func (parser *lphParser) SetResult(pos int, errType int, value string, err error) {
    var errInfo string
    if err != nil {
        errInfo = ErrTypeToErrInfo(errType, err)
    }
    buildStageErrInfo := &BuildStageErrorInfo{
        CmdStrErrorPosition: pos,
        ErrInfo: errInfo,
        ErrType: errType,
        ErrValue: value,
        Err: err,
    }
    parser.result = buildStageErrInfo
}

func (parser *lphParser) TranslatePlaceHolders(input string,
    translatorFn PlaceHolderTranslator) *BuildStageErrorInfo {

    /*create a string scanner to parse the tokens*/
    parser.SetScanner(NewScanner(strings.NewReader(input)))
    tok := parser.NextToken()
    if tok == nil {
        parser.SetResult(0, BIOSTAGE_FAILREASON_GRAMISTAKE, "", BL_ERR_NIL_TOKEN)
        return parser.result
    }
    parser.CleanupCurTokenValue()
searchToken:
    for ;tok != nil; {
        switch tok.tokType {
            case ESCAPE:
                /*The next token shouldn't be considered special*/
                escapedIdent := ""
                err := parser.ParseEscapedToken(&escapedIdent)
                if err != nil {
                    ParserLogger.Infof("Error on parse escaped token: %s\n",
                        err.Error())
                    parser.SetResult(tok.offSet, BIOSTAGE_FAILREASON_GRAMISTAKE, parser.curTokenValue, err)
                    return parser.result
                }
                if strings.HasPrefix(escapedIdent, "$") {
                    parser.curPrefix += escapedIdent
                } else {
                    parser.curPrefix += "\\" + escapedIdent
                }
                parser.SetCurTokenValue(tok.value)
            case WS:
                /*the prefix should be reset*/
                parser.curPrefix += tok.value
                parser.SetCurTokenValue(tok.value)
                stmt := NewIdentStatement(parser.curPrefix)
                parser.curPrefix = ""
                err, endTranslator := translatorFn(stmt)
                if err != nil || !endTranslator {
                    parser.SetResult(tok.offSet, BIOSTAGE_FAILREASON_GRAMISTAKE, parser.curTokenValue, err)
                    return parser.result
                }
                parser.CleanupCurTokenValue()
            case DOLLAR:
                /*
                 * we may encouter a place holder, so:
                 * 1) emit the prefixed ident first
                 * 2) parse and emit the place holder
                 */
                if parser.curPrefix != "" {
                    stmt := NewIdentStatement(parser.curPrefix)
                    parser.curPrefix = ""
                    err, endTranslator := translatorFn(stmt)
                    if err != nil || !endTranslator {
                        parser.SetResult(tok.offSet, BIOSTAGE_FAILREASON_GRAMISTAKE, parser.curTokenValue, err)
                        return parser.result
                    }
                    parser.CleanupCurTokenValue()
                }

                parser.SetCurTokenValue(tok.value)
                /*Parse and emit the place holder*/
                stmt := &PlaceHolderStatement{}
                err := parser.ParsePlaceHolder(stmt)
                if err != nil {
                    ParserLogger.Infof("Error on parse variable: %s\n",
                        err.Error())
                    parser.SetResult(tok.offSet, BIOSTAGE_FAILREASON_GRAMISTAKE, parser.curTokenValue, err)
                    return parser.result
                }
                err, endTranslator := translatorFn(stmt)
                if err != nil || !endTranslator {
                    parser.SetResult(tok.offSet, BIOSTAGE_FAILREASON_GRAMISTAKE, parser.curTokenValue, err)
                    return parser.result
                }
                parser.CleanupCurTokenValue()
            case EOF:
                break searchToken
            default:
                /*just receive the string*/
                parser.curPrefix += tok.value
                parser.SetCurTokenValue(tok.value)
        }
        
        tok = parser.NextToken()
    }

    if parser.curPrefix != "" {
        identStmt := NewIdentStatement(parser.curPrefix)
        err, _ := translatorFn(identStmt)
        if err != nil {
            parser.SetResult(tok.offSet, BIOSTAGE_FAILREASON_GRAMISTAKE, parser.curTokenValue, err)
            return parser.result
        }
        parser.CleanupCurTokenValue()
    }

    parser.SetResult(TOKEN_HEAD, -1, "", nil)
    return parser.result
}

func (parser *lphParser) ParsePlaceHolder(stmt *PlaceHolderStatement) error {
    /*
     * Parse the detailed variable in the following format:
     * 1) $keyword.tag
     * 2) $keywordNumber.tag
     * 3) $keyowrdNumber
     * 4) $xyz
     * 5) $inputs.bam.splitjoinprefix{'-I '}
     * 6) $files.*.splitjoinprefix{'-I '}
     */
   
    stmt.stmtType = BL_INVALID
    endTok := WS
    tok := parser.NextToken()
    tokPos := 1
    dotCount := 0

parsePlaceHolder:
    for ;tok != nil; {
        parser.SetCurTokenValue(tok.value)
        switch tok.tokType {
            case LBRACE:
                if tokPos != 1 {
                    errInfo := BuildBlPaserError(tok.offSet, "", BIOLPH_PARSER_LBRACE_VALUE)
                    return errors.New(errInfo)
                }
                endTok = RBRACE
            case WS:
                if endTok != WS {
                    errInfo := BuildBlPaserError(tok.offSet, "", BIOLPH_PARSER_WS_VALUE)
                    return errors.New(errInfo)
                }
                parser.PushBackNextToken(tok)
                break parsePlaceHolder
            case RBRACE:
                if endTok != RBRACE {
                    errInfo := BuildBlPaserError(tok.offSet, "", BIOLPH_PARSER_RBRACE_VALUE)
                    return errors.New(errInfo)
                }
                break parsePlaceHolder
            case DOT:
                if stmt.stmtType == BL_INVALID {
                    stmt.value += tok.value
                } else {
                    dotCount ++

                    /*
                     * Check the $inputs.bam.splitjoinprefix{'sdafda'}
                     * like place holders
                     *
                     */
                    if (stmt.stmtType == BL_EXPAND_INPUT || stmt.stmtType == BL_EXPAND_FILE) && dotCount == 2 {
                        err := parser.ParsePlaceHolderOperation(stmt)
                        if err != nil {
                            ParserLogger.Infof("Parse place holder operation failure: %s\n",
                                err.Error())
                            return err
                        }
                    }
                }
            case INPUT:
                if tokPos <=2 {
                    stmt.value = ""
                    stmt.stmtType = BL_TAG_INPUT
                } else {
                    stmt.value += tok.value
                }
            case INDEXEDINPUT:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_INDEX_INPUT
                    stmt.index = tok.digit
                } else {
                    stmt.value += tok.value + fmt.Sprintf("%s", tok.digit)
                }
            case OUTPUT:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_TAG_OUTPUT
                } else {
                    stmt.value += tok.value
                }
            case OUTPUTPATH:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_TAG_OUTPUT_PATH
                } else {
                    stmt.value += tok.value
                }
            case OUTPUTDIR:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_TAG_OUTPUTDIR
                } else {
                    stmt.value += tok.value
                }
            case OUTPUTDIRPATH:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_TAG_OUTPUTDIR_PATH
                } else {
                    stmt.value += tok.value
                }
            case FILE:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_TAG_REF
                } else {
                    stmt.value += tok.value
                }
            case INDEXEDFILE:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_INDEX_FILE
                    stmt.index = tok.digit
                } else {
                    stmt.value += tok.value + fmt.Sprintf("%s", tok.digit)
                }
            case INPUTS:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_EXPAND_INPUT
                } else {
                    stmt.value += tok.value
                }
            case FILES:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_EXPAND_FILE
                } else {
                    stmt.value += tok.value
                }
            case SYS:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_SYS_DIR
                } else {
                    stmt.value += tok.value
                }
            case BRANCH:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_TAG_BRANCH
                } else {
                    stmt.value += tok.value
                }
            case INPUTDIR:
                if tokPos <= 2 {
                    stmt.value = ""
                    stmt.stmtType = BL_TAG_INPUT_DIR
                } else {
                    stmt.value += tok.value
                }
            case SLASH:
                /*
                 * The slash should break parsing dir to support:
                 * 1) $inputdir/xx/yy.../...
                 * 2) $sys.workdir/xx/...
                 */
                if stmt.stmtType == BL_TAG_INPUT_DIR || 
                    stmt.stmtType == BL_SYS_DIR {
                    parser.PushBackNextToken(tok)
                    break parsePlaceHolder
                } else {
                    stmt.value += tok.value
                }
            case IDENT:
                stmt.value += tok.value
            case ASTERISK:
                if stmt.stmtType != BL_EXPAND_FILE && 
                    stmt.stmtType != BL_EXPAND_INPUT {
                    errInfo := BuildBlPaserError(tok.offSet, "", BIOLPH_PARSER_ASTERISK_VALUE)
                    return errors.New(errInfo)
                }
            case EOF:
                break parsePlaceHolder
            default:
                stmt.value += tok.value
        }

        tok = parser.NextToken()
        tokPos ++
    }

    if stmt.stmtType == BL_INVALID {
        stmt.stmtType = BL_TAG_VARIABLE
    }

    /*
     * Validate the placeholder statement
     */
    if stmt.stmtType == BL_SYS_DIR {
        val := strings.ToUpper(stmt.value)
        switch val {
            case BL_KW_SYSWORKDIR, BL_KW_SYSINPUTDIR, BL_KW_SPARKMASTER,
                BL_KW_SYSJOBWORKDIR:
            default:
                stmt.stmtType = BL_INVALID
                errInfo := BuildBlPaserError(tok.offSet, val, BIOLPH_PARSER_SYSDIR_VALUE)
                return errors.New(errInfo)
        }
    }

    return nil
}

/*
 * The expand style place holders (e.g, inputs.bam) may be suffixed a
 * operation to help build complicated command string. for example:
 *  $inputs.bam.splitjoinprefix{'-I '} will be expand to : 
 *    -I /root/a.bam -I /root/b.bam -I /root/c.bam
 * 
 * This routine is to parse: opname{'xxxxx'}. we strictly require user
 * input it by format: OPName{'IDENT|WS'}
 */
func (parser *lphParser) ParsePlaceHolderOperation(stmt *PlaceHolderStatement) error {

    tok := parser.NextToken()
    tokCount := 1
    stmt.prefix = ""
    quoteCount := 0
    lBraced := false
    rBraced := false
    stmt.op = BL_OP_INVALID

searchOperation:
    for ;tok != nil; {
        parser.SetCurTokenValue(tok.value)
        switch tok.tokType {
            case OP_SPLITJOINPREFIX:
                if tokCount != 1 {
                    errInfo := BuildBlPaserError(tok.offSet, tok.value, BIOLPH_PARSER_SPLITJOINPREFIX_VALUE)
                    return errors.New(errInfo)
                }
                stmt.op = BL_OP_SPLITJOINPREFIX
            case LBRACE:
                if tokCount != 2 {
                    errInfo := BuildBlPaserError(tok.offSet, tok.value, BIOLPH_PARSER_SPLITJOINPREFIX_VALUE)
                    return errors.New(errInfo)
                }
                lBraced = true
            case RBRACE:
                rBraced = true
                break searchOperation
            case EOF:
                break searchOperation
            case ESCAPE:
                escapedIdent := ""
                parser.ParseEscapedToken(&escapedIdent)
                if escapedIdent != "" {
                    if strings.HasPrefix(escapedIdent, "$") {
                        parser.curPrefix += escapedIdent
                    } else {
                        stmt.prefix += "\\" + escapedIdent
                    }
                }
            case QUOTE:
                /*we need igore it*/
                quoteCount ++
            case WS:
                stmt.prefix += tok.value
            case IDENT:
                stmt.prefix += tok.value
            default:
                stmt.prefix += tok.value
        }

        tok = parser.NextToken()
        tokCount ++
    }

    if quoteCount != 2 {
        ParserLogger.Errorf("The quote in place holder operation should be matched: %d\n",
            quoteCount)
        errInfo := BuildBlPaserError(tok.offSet, tok.value, BIOLPH_PARSER_SPLITJOINPREFIX_VALUE)
        return errors.New(errInfo)
    }

    if !lBraced || !rBraced {
        ParserLogger.Errorf("The {} should match exactly in operation\n")
        errInfo := BuildBlPaserError(tok.offSet, tok.value, BIOLPH_PARSER_SPLITJOINPREFIX_VALUE)
        return errors.New(errInfo)
    }

    if stmt.op == BL_OP_INVALID {
        ParserLogger.Infof("No valid operation exist\n")
        errInfo := BuildBlPaserError(tok.offSet, tok.value, BIOLPH_PARSER_SPLITJOINPREFIX_VALUE)
        return errors.New(errInfo)
    }

    return nil
}

func (parser *lphParser) ParseEscapedToken(escapedIdent *string) error {
    /*
     * any token should be igonred
     */
    tok := parser.NextToken()
    if tok == nil {
        return errors.New("Parser encounter nil token")
    }

    if tok == NewToken(EOF, "", tok.offSet) {
        return nil
    } else {
        *escapedIdent = tok.value
        return nil
    }
}
