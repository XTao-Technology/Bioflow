package common

import (
    "os/exec"
    "time"
    "errors"
)

func RunCommandAsync(command string, timeout int) error {
    cmd := exec.Command("/bin/sh", "-c", command)
    if err := cmd.Start(); err != nil {
        StoneLogger.Printf("fail to run the command %s: %s\n",
            command, err.Error())
        return err
    }

    /*timeout <=0 means we need block wait command to complete*/
    if timeout <= 0 {
        if err := cmd.Wait(); err != nil {
            StoneLogger.Printf("fail to wait command %s: %s\n",
                command, err.Error())
            return err
        }
    } else {
        waitChan := make(chan struct {})
        var waitErr error = nil
        go func() {
            waitErr = cmd.Wait()
            if waitErr != nil {
                StoneLogger.Printf("fail to wait the command %s: %s\n",
                    command, waitErr.Error())
            }

            close(waitChan)
        } ()

        select {
            case <- time.After(time.Duration(timeout) * time.Second):
                StoneLogger.Printf("Timeout on exec command %s\n",
                    command)
                return errors.New("The command timeout")
            case <- waitChan:
                return waitErr
        }
    }

    return nil
}
