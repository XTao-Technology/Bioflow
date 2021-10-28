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
)

/*
 * Token represents a lexical token, 
 * it has tree types:
 * 1) ident, special character
 * 2) keywords
 * 3) indexed keywords like: input1, input2,
 *    in this case, value has keyword and digit
 *    has the index value
 */

type Token struct {
    tokType int
    value string
    digit int
    offSet int
}

func NewToken(tokType int, value string, offSet int) *Token {
    return &Token{
            tokType: tokType,
            value: value,
            digit: -1,
            offSet: offSet,
    }
}

func NewIndexedInput(value string, index int, offSet int) *Token {
    return &Token{
            tokType: INDEXEDINPUT,
            value: value,
            digit: index,
            offSet: offSet,
    }
}

func NewIndexedFile(value string, index int, offSet int) *Token {
    return &Token{
            tokType: INDEXEDFILE,
            value: value,
            digit: index,
            offSet: offSet,
    }
}

/*
var (
    ILLEGAL_TOKEN *Token = NewToken(ILLEGAL, "")
    EOF_TOKEN *Token = NewToken(EOF, "")
    ESCAPE_TOKEN *Token = NewToken(ESCAPE, "\\")
    LBRACE_TOKEN *Token = NewToken(LBRACE, "{")
    RBRACE_TOKEN *Token = NewToken(RBRACE, "}")
    DOT_TOKEN *Token = NewToken(DOT, ".")
    DOLLAR_TOKEN *Token = NewToken(DOLLAR, "$")
    ASTERISK_TOKEN *Token = NewToken(ASTERISK, "*")
    QUOTE_TOKEN *Token = NewToken(QUOTE, "'")
    SLASH_TOKEN *Token = NewToken(SLASH, "/")
)
*/

const (
	/* Special tokens */
	ILLEGAL int = iota
    SPECIAL                        /* \\ , */ 
	EOF
	WS

	/* Literals */
	IDENT                          /* abc */

	/* Misc characters */
	ASTERISK                       /* * */
    DOLLAR                         /* $ */
    LBRACE                         /* { */
    RBRACE                         /* } */
    ESCAPE                         /* \ */
    DOT                            /* . */
    QUOTE                          /* ' */
    SLASH                          /* / */

	/* Keywords */
	INPUT
    INDEXEDINPUT 
    INPUTDIR
	INPUTS
    FILE
    INDEXEDFILE
    FILES
    BRANCH
    SYS
    OUTPUT
    OUTPUTPATH
    OUTPUTDIR
    OUTPUTDIRPATH

    /* Operations */
    OP_SPLITJOINPREFIX
)

const TOKEN_HEAD int = 0
