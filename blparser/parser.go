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
    "errors"
    "fmt"
)

const (
    BL_STMT_STR int = 0                  /*a string, no action*/
    BL_STMT_TAG_INPUT int = 1            /*XY$input.tag, replace with tag*/
    BL_STMT_TAG_OUTPUT int = 2           /*XY$output.tag, replace with tag*/
    BL_STMT_TAG_REF int = 3              /*reference, replace with file*/
    BL_STMT_EXPAND_FILE int = 4          /*XY$files.*, expand like XYfile1 XYfile2*/
    BL_STMT_EXPAND_INPUT int = 5         /*XY$input.*, expand like XYfile1 XYfile2*/
    BL_STMT_TAG_VARIABLE int = 6         /*XY$variable.*, expand like XYval1 XYval2*/
    BL_STMT_TAG_BRANCH int = 7           /*XY$branch.Z, expand like XYval1 XYval2*/
    BL_STMT_SYS_DIR int = 8              /*XY$sys.dir, expand to XY/a/b/../c*/
    BL_STMT_TAG_INPUT_DIR int = 9        /*XY$inputdir.tag, expand to XY/a/b/../c*/
    BL_STMT_INDEX_INPUT int = 10
    BL_STMT_INDEX_FILE int = 11
    BL_STMT_TAG_OUTPUT_PATH int = 12          /*XY$outputpath.tag*/
    BL_STMT_TAG_OUTPUTDIR int = 13            /*XY$outputdir.tag*/
    BL_STMT_TAG_OUTPUTDIR_PATH int = 14       /*XY$outputdirpath.tag*/
)

type BLStmtType int 

func (stmtType BLStmtType) IsError() bool {
    return int(stmtType) == -1
}

func (stmtType BLStmtType) IsStr() bool {
    return int(stmtType) == BL_STMT_STR
}

func (stmtType BLStmtType) IsTagInput() bool {
    return int(stmtType) == BL_STMT_TAG_INPUT
}

func (stmtType BLStmtType) IsTagOutput() bool {
    return int(stmtType) == BL_STMT_TAG_OUTPUT
}

func (stmtType BLStmtType) IsTagOutputPath() bool {
    return int(stmtType) == BL_STMT_TAG_OUTPUT_PATH
}

func (stmtType BLStmtType) IsTagRef() bool {
    return int(stmtType) == BL_STMT_TAG_REF
}

func (stmtType BLStmtType) IsTagVariable() bool {
    return int(stmtType) == BL_STMT_TAG_VARIABLE
}

func (stmtType BLStmtType) IsExpandFile() bool {
    return int(stmtType) == BL_STMT_EXPAND_FILE
}

func (stmtType BLStmtType) IsExpandInput() bool {
    return int(stmtType) == BL_STMT_EXPAND_INPUT
}

func (stmtType BLStmtType) IsTagBranch() bool {
    return int(stmtType) == BL_STMT_TAG_BRANCH
}

func (stmtType BLStmtType) IsSysDir() bool {
    return int(stmtType) == BL_STMT_SYS_DIR
}

func (stmtType BLStmtType) IsOutputDir() bool {
    return int(stmtType) == BL_STMT_TAG_OUTPUTDIR
}

func (stmtType BLStmtType) IsOutputDirPath() bool {
    return int(stmtType) == BL_STMT_TAG_OUTPUTDIR_PATH
}

func (stmtType BLStmtType) IsTagInputDir() bool {
    return int(stmtType) == BL_STMT_TAG_INPUT_DIR
}

func (stmtType BLStmtType) IsTagIndexInput() bool {
    return int(stmtType) == BL_STMT_INDEX_INPUT
}

func (stmtType BLStmtType) IsIndexFile() bool {
    return int(stmtType) == BL_STMT_INDEX_FILE
}

const (
    BL_STR BLStmtType = BLStmtType(BL_STMT_STR)
    BL_TAG_INPUT BLStmtType = BLStmtType(BL_STMT_TAG_INPUT)
    BL_TAG_INPUT_DIR BLStmtType = BLStmtType(BL_STMT_TAG_INPUT_DIR)
    BL_TAG_REF BLStmtType = BLStmtType(BL_STMT_TAG_REF)
    BL_TAG_VARIABLE BLStmtType = BLStmtType(BL_STMT_TAG_VARIABLE)
    BL_TAG_OUTPUT BLStmtType = BLStmtType(BL_STMT_TAG_OUTPUT)
    BL_TAG_OUTPUT_PATH BLStmtType = BLStmtType(BL_STMT_TAG_OUTPUT_PATH)
    BL_TAG_OUTPUTDIR BLStmtType = BLStmtType(BL_STMT_TAG_OUTPUTDIR)
    BL_TAG_OUTPUTDIR_PATH BLStmtType = BLStmtType(BL_STMT_TAG_OUTPUTDIR_PATH)
    BL_TAG_BRANCH BLStmtType = BLStmtType(BL_STMT_TAG_BRANCH)
    BL_SYS_DIR BLStmtType = BLStmtType(BL_STMT_SYS_DIR)
    BL_EXPAND_FILE BLStmtType = BLStmtType(BL_STMT_EXPAND_FILE)
    BL_EXPAND_INPUT BLStmtType = BLStmtType(BL_STMT_EXPAND_INPUT)
    BL_INDEX_INPUT BLStmtType = BLStmtType(BL_STMT_INDEX_INPUT)
    BL_INDEX_FILE BLStmtType = BLStmtType(BL_STMT_INDEX_FILE)
    BL_INVALID BLStmtType = BLStmtType(-1)
)

/*Parser error definition*/
var (
    BL_ERR_NIL_TOKEN error = errors.New("A nil token received")
)

/*syntax error*/
var SYNTAX_PERFIX string = "syntax error"
const (
    BIOLPH_PARSER_LBRACE_VALUE string = "unexpected { not after $"
    BIOLPH_PARSER_WS_VALUE string = "the { and } not match"
    BIOLPH_PARSER_RBRACE_VALUE string = "the { and } not match"
    BIOLPH_PARSER_ASTERISK_VALUE string = "only support files.* or inputs.*"
    BIOLPH_PARSER_SYSDIR_VALUE string = "Only support $sys.indir, $sys.workdir, $sys.sparkmaster"
    BIOLPH_PARSER_SPLITJOINPREFIX_VALUE string = "not in the format splitjoinprefix.{'IDENT'} strictly"

)

func BuildBlPaserError(position int, tok_value string, value string) string {
    if tok_value != "" {
        return _BuildBlPaserError(position, tok_value, value)
    } else {
        return fmt.Sprintf("position:%d: %s: %s",
            position, SYNTAX_PERFIX, value)
    }
}

func _BuildBlPaserError(position int, tok_value string, value string) string {
    return fmt.Sprintf("position:%d: %s: unexpected '%s', %s",
        position, SYNTAX_PERFIX, tok_value, value)
}

/*
 * The interface to represent any bioflow pipeline
 * definition language statement
 */
type BLStatement interface {
    StmtType() BLStmtType
    Value() string
    Prefix() string
    SetPrefix(string)
    Index() int
    SetIndex(index int)
}

const (
    BL_OP_INVALID int = iota
    BL_OP_SPLITJOINPREFIX
)

type PlaceHolderStatement struct {
    stmtType BLStmtType
    value string
    prefix string
    op int
    index int
}

func (stmt *PlaceHolderStatement) StmtType() BLStmtType {
    return stmt.stmtType
}

func (stmt *PlaceHolderStatement) Value() string {
    return stmt.value
}

func (stmt *PlaceHolderStatement) Prefix() string {
    return stmt.prefix
}

func (stmt *PlaceHolderStatement) SetPrefix(prefix string) {
    stmt.prefix = prefix
}

func (stmt *PlaceHolderStatement) Index() int {
    return stmt.index
}

func (stmt *PlaceHolderStatement) SetIndex(index int) {
    stmt.index = index
}

type IdentStatement struct {
    value string
}

func NewIdentStatement(value string) *IdentStatement {
    return &IdentStatement{
            value: value,
    }
}

func (stmt *IdentStatement) StmtType() BLStmtType {
    return BL_STR
}

func (stmt *IdentStatement) Value() string {
    return stmt.value
}

func (stmt *IdentStatement) Prefix() string {
    return ""
}

func (stmt *IdentStatement) SetPrefix(prefix string) {
    return
}

func (stmt *IdentStatement) Index() int {
    return -1
}

func (stmt *IdentStatement) SetIndex(index int) {
    return
}

type PlaceHolderTranslator func(BLStatement) (error, bool)
