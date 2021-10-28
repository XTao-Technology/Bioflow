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
	"bufio"
	"bytes"
	"io"
	"strconv"
	"strings"
)

/* Scanner represents a lexical scanner.*/
type Scanner struct {
	r *bufio.Reader
	offSet int
}

/* NewScanner returns a new instance of Scanner.*/
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		r: bufio.NewReader(r),
		offSet: -1,
    }
}

/* Scan returns the next token and literal value.*/
func (s *Scanner) Scan() *Token {
	/* Read the next rune.*/
	tokOffset := s.offSet
	ch := s.read()

	/* If we see whitespace then consume all contiguous whitespace.
	 * If we see a letter then consume as an ident or reserved word.
	 * If we see a digit then consume as a number.
	 */
	if isWhitespace(ch) {
		s.unread()
		return s.scanWhitespace()
	} else if isLetter(ch) {
		s.unread()
		return s.scanIdent()
	}

	/* Otherwise read the individual character.*/
	switch ch {
	case eof:
		return NewToken(EOF, "", tokOffset)
	case '*':
		return NewToken(ASTERISK, "*", tokOffset)
	case '{':
		return NewToken(LBRACE, "{", tokOffset)
	case '}':
		return NewToken(RBRACE, "}", tokOffset)
	case '\\':
		return NewToken(ESCAPE, "\\", tokOffset)
	case '$':
		return NewToken(DOLLAR, "$", tokOffset)
	case '.':
		return NewToken(DOT, ".", tokOffset)
	case '\'':
		return NewToken(QUOTE, "'", tokOffset)
	case '/':
		return NewToken(SLASH, "/", tokOffset)
	}

	return NewToken(SPECIAL, string(ch), tokOffset)
}

/*
 * scanWhitespace consumes the current rune and all contiguous whitespace.
 */
func (s *Scanner) scanWhitespace() *Token {
	/* Create a buffer and read the current character into it. */
	var buf bytes.Buffer
	tokOffset := s.offSet
	buf.WriteRune(s.read())

	/*
	 * Read every subsequent whitespace character into the buffer.
	 * Non-whitespace characters and EOF will cause the loop to exit.
	 */
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.unread()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	return NewToken(WS, buf.String(), tokOffset)
}

/* scanIdent consumes the current rune and all contiguous ident runes.*/
func (s *Scanner) scanIdent() *Token {
	/* Create a buffer and read the current character into it.*/
	var buf bytes.Buffer
	tokOffset := s.offSet
	buf.WriteRune(s.read())

	/* Read every subsequent ident character into the buffer.
	 * Non-ident characters and EOF will cause the loop to exit.
	 * This is to identify the keyword first
	 */
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isLetter(ch) {
			s.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	prefixToken := IDENT
	/* If the string matches a keyword then return that keyword.*/
	switch strings.ToUpper(buf.String()) {
	case BL_KW_INPUT:
		prefixToken = INPUT
	case BL_KW_INPUTDIR:
		return NewToken(INPUTDIR, buf.String(), tokOffset)
	case BL_KW_INPUTS:
		return NewToken(INPUTS, buf.String(), tokOffset)
	case BL_KW_FILE:
		prefixToken = FILE
	case BL_KW_FILES:
		return NewToken(FILES, buf.String(), tokOffset)
	case BL_KW_BRANCH:
		return NewToken(BRANCH, buf.String(), tokOffset)
	case BL_KW_SYS:
		return NewToken(SYS, buf.String(), tokOffset)
	case BL_KW_OUTPUT:
		return NewToken(OUTPUT, buf.String(), tokOffset)
	case BL_KW_OUTPUT_PATH:
		return NewToken(OUTPUTPATH, buf.String(), tokOffset)
    case BL_KW_OUTPUTDIR:
        return NewToken(OUTPUTDIR, buf.String(), tokOffset)
    case BL_KW_OUTPUTDIR_PATH:
        return NewToken(OUTPUTDIRPATH, buf.String(), tokOffset)
	case BL_KW_SPLITJOINPREFIX:
		return NewToken(OP_SPLITJOINPREFIX, buf.String(), tokOffset)
	}

	var suffixBuf bytes.Buffer
	for {
		if ch := s.read(); ch == eof {
			break
		} else if !isLetter(ch) && !isDigit(ch) && ch != '_' {
			s.unread()
			break
		} else {
			_, _ = suffixBuf.WriteRune(ch)
		}
	}

	if prefixToken == IDENT {
	    /* Case 1: ident
	     */
	    return NewToken(IDENT, buf.String() + suffixBuf.String(), tokOffset)
	} else if prefixToken == INPUT || prefixToken == FILE {
	    if suffixBuf.String() == "" {
	        if prefixToken == INPUT {
	            return NewToken(INPUT, buf.String(), tokOffset)
	        } else {
	            return NewToken(FILE, buf.String(), tokOffset)
	        }
	    }
	    /*
	     * Case 2: try to scan a keyword like INPUT1, INPUT2, ...
	     */
	    index, err := strconv.Atoi(suffixBuf.String())
	    if err == nil {
	        if prefixToken == INPUT {
	            return NewIndexedInput(buf.String(), index, tokOffset)
	        } else {
	            return NewIndexedFile(buf.String(), index, tokOffset)
	        }
	    } else {
	        return NewToken(IDENT, buf.String() + suffixBuf.String(), tokOffset)
	    }
	} else {
	    return NewToken(ILLEGAL, "",  tokOffset)
	}
}

/*
 * read reads the next rune from the bufferred reader.
 * Returns the rune(0) if an error occurs (or io.EOF is returned).
 */
func (s *Scanner) read() rune {
	ch, _, err := s.r.ReadRune()
	if err != nil {
		return eof
	}
	s.offSet ++
	return ch
}

/* unread places the previously read rune back on the reader.*/
func (s *Scanner) unread() {
    _ = s.r.UnreadRune()
    s.offSet --
}

/* isWhitespace returns true if the rune is a space, tab, or newline.*/
func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

/* isLetter returns true if the rune is a letter.*/
func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

/* isDigit returns true if the rune is a digit.*/
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

/* eof represents a marker rune for the end of the reader. */
var eof = rune(0)
