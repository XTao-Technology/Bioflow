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
    "crypto/aes"
    "crypto/cipher"
	"encoding/hex"
)

type AesEncrypt struct {
	keyStr string
}

const (
	BIOFLOW_API_KEY_SEED = "B10f10wApiKeySeed"
	BIOFLOW_SECURITY_KEY_SEED = "B10f10wSecretKeySeed"
)

func NewAesEncrypt(key string) *AesEncrypt{
	return &AesEncrypt {
		keyStr: key,
	}
}

func (this *AesEncrypt) getKey() []byte {
    strKey := this.keyStr
    keyLen := len(strKey)
    if keyLen < 16 {
		panic("Key length is less than 16!")
    }
    arrKey := []byte(strKey)
    if keyLen >= 32 {
        // first 32 bytes
        return arrKey[:32]
    }
    if keyLen >= 24 {
        // first 24 bytes
        return arrKey[:24]
    }
    // first 16 bytes
    return arrKey[:16]
}

// Encrypt string to a secret string
func (this *AesEncrypt) Encrypt(strMesg string) (string, error) {
    key := this.getKey()
    var iv = []byte(key)[:aes.BlockSize]
    encrypted := make([]byte, len(strMesg))
    aesBlockEncrypter, err := aes.NewCipher(key)
    if err != nil {
        return "", err
    }
    aesEncrypter := cipher.NewCFBEncrypter(aesBlockEncrypter, iv)
    aesEncrypter.XORKeyStream(encrypted, []byte(strMesg))
    return hex.EncodeToString(encrypted), nil
}

// Decode secret string to string
func (this *AesEncrypt) Decrypt(src string) (strDesc string, err error) {
    defer func() {
        if e := recover(); e != nil {
            err = e.(error)
        }
    }()

	srcCode, err := hex.DecodeString(src)
	if err != nil {
		return "", err
	}
    key := this.getKey()
    var iv = []byte(key)[:aes.BlockSize]
    decrypted := make([]byte, len(srcCode))
    var aesBlockDecrypter cipher.Block
    aesBlockDecrypter, err = aes.NewCipher([]byte(key))
    if err != nil {
        return "", err
    }
    aesDecrypter := cipher.NewCFBDecrypter(aesBlockDecrypter, iv)
    aesDecrypter.XORKeyStream(decrypted, srcCode)
    return string(decrypted), nil
}

/*
func main() {
    aesEnc := NewAesEncrypt("b10f10wapikeyseed")
    arrEncrypt, err := aesEnc.Encrypt("abcde")
    if err != nil {
        fmt.Println(arrEncrypt)
        return
    }
	fmt.Println(arrEncrypt)

    strMsg, err := aesEnc.Decrypt(arrEncrypt)
    if err != nil {
        fmt.Println(arrEncrypt)
        return
    }
    fmt.Println(strMsg)
}
*/
