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

package common

import (
	"os"
	"fmt"
	"errors"
	"io/ioutil"
)
const (
	BIOFLOW_CLIENT_ADMIN_PASSFILE = "/etc/.bioflow_admin.key"
	BIOFLOW_INITIAL_ADMIN_PASSWORD string = "b10f10w@x7a0"
	BIOFLOW_ADMIN_PASSWORD_SEED string = "b10f10w@xta01sthefuture"
	BIOFLOW_ADMIN_PASSWORD_SHADOW string = "b10f10w@xta0Shad0w"
)

/*
 * encrypt admin passwd by BIOFLOW_ADMIN_PASSWORD_SHADOW at local
 * encrypt admin passwd by BIOFLOW_ADMIN_PASSWORD_SEED at ETCD
 * admin skey = encrypt admin passwd by BIOFLOW_SECURITY_KEY_SEED
 */

func SaveAdminPassword(pass string) error {
	tmpfile, err := ioutil.TempFile(ClientConfDir(), "bioflow_adminpass")
	if err != nil {
		fmt.Println("Fail to create temp password file", err)
		return err
	}
	defer os.Remove(tmpfile.Name())

	aesPass := NewAesEncrypt(BIOFLOW_ADMIN_PASSWORD_SHADOW)
	passEnc, err := aesPass.Encrypt(pass)

	if err != nil {
		fmt.Println("Failed to encrypt password!")
		return err
	}

	if _, err := tmpfile.WriteString(passEnc); err != nil {
		fmt.Println("Write temp password error!", err)
		return err
	}

	err = os.Rename(tmpfile.Name(), BIOFLOW_CLIENT_ADMIN_PASSFILE)
	if err != nil {
		fmt.Println("Failed to rename to bioflow client admin passfile:%s!\n", err.Error())
		return err
	}

	tmpfile.Close()

	return nil
}

func LoadAdminPassword() (string, error) {
	_, err := os.Stat(BIOFLOW_CLIENT_ADMIN_PASSFILE)
	if os.IsNotExist(err) {
		return "", nil
	}

	o, err := ioutil.ReadFile(BIOFLOW_CLIENT_ADMIN_PASSFILE)
	if err != nil {
		return "", errors.New("Failed to read password file!")
	}

	aesPass := NewAesEncrypt(BIOFLOW_ADMIN_PASSWORD_SHADOW)
	pass, err := aesPass.Decrypt(string(o))
	if err != nil {
		return "", errors.New("Failed to decrypt password.")
	}

	return pass, nil
}

func LoadAdminSkey() (string, error) {

	_, err := os.Stat(BIOFLOW_CLIENT_ADMIN_PASSFILE)
	if os.IsNotExist(err) {
		return BIOFLOW_INITIAL_ADMIN_PASSWORD, nil
	}

	o, err := ioutil.ReadFile(BIOFLOW_CLIENT_ADMIN_PASSFILE)
	if err != nil {
		fmt.Println("Failed to read password file.", err.Error())
		return "", errors.New("Failed to read password file.")
	}

	aesPass := NewAesEncrypt(BIOFLOW_ADMIN_PASSWORD_SHADOW)
	pass, err := aesPass.Decrypt(string(o))
	if err != nil {
		fmt.Println("Failed to decrypt shadow passward.", err.Error())
		return "", errors.New("Failed to decrypt shadow password.")
	}

	aesSkey := NewAesEncrypt(BIOFLOW_SECURITY_KEY_SEED)
	skey, err := aesSkey.Encrypt(pass)
	if err != nil {
		fmt.Println("Failed to get skey for admin.", err.Error())
		return "", errors.New("Failed to get security key for admin.")
	}

	return skey, err
}
