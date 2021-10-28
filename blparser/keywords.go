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

/*
 * define the keywords of bioflow pipeline here
 */
const (
    BL_VAR_NAME string = "name"
    BL_VAR_SHARDGROUP string = "shardgroup"
    BL_VAR_SAMPLE string = "sample"
)

const (
    BL_KW_INPUT string = "INPUT"
    BL_KW_OUTPUT string = "OUTPUT"
    BL_KW_OUTPUT_PATH string = "OUTPUTPATH"
    BL_KW_OUTPUTDIR string = "OUTPUTDIR"
    BL_KW_OUTPUTDIR_PATH string = "OUTPUTDIRPATH"
    BL_KW_INPUTDIR string = "INPUTDIR"
    BL_KW_SYS string = "SYS"
    BL_KW_FILE string = "FILE"
    BL_KW_FILES string = "FILES"
    BL_KW_SYSWORKDIR string = "WORKDIR"
    BL_KW_SYSJOBWORKDIR string = "JOBWORKDIR"
    BL_KW_SYSINPUTDIR string = "INDIR"
    BL_KW_SPARKMASTER string = "SPARKMASTER"
    BL_KW_INPUTS string = "INPUTS"
    BL_KW_BRANCH string = "BRANCH"
)

const (
    BL_KW_SPLITJOINPREFIX string = "SPLITJOINPREFIX"
)
