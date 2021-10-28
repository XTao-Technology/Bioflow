package client

import (
    "fmt"
    "strings"
    "net/http"
    "encoding/json"
    . "github.com/xtao/bioflow/common"
)

func SignClientReqHeader(req *http.Request, account *UserAccountInfo) error {
    var user, group, uid, gid, umask string

    signedHeaders := []string{}

    if _, exists := req.Header["Content-Type"]; exists {
        signedHeaders = append(signedHeaders, "Content-Type")
    }
    accHeader := make(map[string]interface{})

    user = UID_GLOBAL
    if account.Username != "" {
        user = account.Username
    }

    accHeader["user"] = user

    AesAkey := NewAesEncrypt(BIOFLOW_API_KEY_SEED)
    AesSkey := NewAesEncrypt(BIOFLOW_SECURITY_KEY_SEED)
    aKey, err := AesAkey.Encrypt(user)
    if err != nil {
        return err
    }

    var sKey string

    if strings.ToUpper(user) == "ROOT" {
        sKey, err = LoadAdminSkey()
    } else {
        sKey, err = AesSkey.Encrypt(user)
    }
    if err != nil {
        return err
    }

    if account.Groupname != "" {
        group = account.Groupname
        accHeader["group"] = group
    }

    if account.Uid != "" {
        uid = account.Uid
        accHeader["uid"] = uid
    }

    if account.Gid != "" {
        gid = account.Gid
        accHeader["gid"] = gid
    }

    if account.Umask != "" {
        umask = account.Umask
        accHeader["umask"] = umask
    }

    js, err := json.Marshal(accHeader)
    if err != nil {
        return err
    }

    // Encrypt the account information
    dKey := sKey
    if len(dKey) < 16 {
        dKey = fmt.Sprintf("%16s", sKey)
    }

    aesEnc := NewAesEncrypt(dKey)
    aHeader, err := aesEnc.Encrypt(string(js))
    if err != nil {
        return err
    }

    signedHeaders = append(signedHeaders, AccountHeader)
    req.Header.Add(AccountHeader, aHeader)

    options := Options {
        SignedHeaders: signedHeaders,
    }

    str, err := StringToSign(req, &options)
    if err != nil {
        return err
    }

    signature := SignString(str, sKey)

    authHeader := fmt.Sprintf("APIKey=%s,Signature=%s", aKey, signature)
    req.Header.Add("Authorization", authHeader)

    return nil
}
