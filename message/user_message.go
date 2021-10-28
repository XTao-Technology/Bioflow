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
package message

type BioflowUserInfo struct {
    Username string
    Uid string
    Gid string
    Groupname string
    JobNum int64
    TaskNum int64
    PausedJobNum int64
    CompletedJobNum int64
    LostTaskNum int64
    CanceledJobNum int64
    FailJobNum int64
    RunningJobNum int64
    Credit int64
    JobQuota int64
    TaskQuota int64
    Mail string
    Groups map[string]bool
}

type BioflowGetUserInfoResult struct {
	Status string
	Msg string
	UserInfo BioflowUserInfo
}

type BioflowListUserInfoResult struct {
	Status string
	Msg string
	UserList []BioflowUserInfo
}

type BioflowUserConfig struct {
    ID string
    Credit int64
    JobQuota int64
    TaskQuota int64
    Mail string
}

type UserRscStatsOpt struct {
	UserId string `json:"user"`
	After string  `json:"after"`
	Before string `json:"before"`
}

type UserRscStats struct {
	ID string
	JobCnt uint64
	StageCnt uint64
	CpuMinutes float64
	MemMinutes float64
}

type BioflowListUserRscStatsResult struct {
	Status string
	Msg string
	StatsList []UserRscStats
}
