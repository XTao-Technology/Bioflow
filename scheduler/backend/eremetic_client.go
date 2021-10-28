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
package backend

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
    "io"
    "os"
	"net/http"
    "errors"

	"github.com/klarna/eremetic"
    xcommon "github.com/xtao/xstone/common"
)

// Client is used for communicating with an Eremetic server.
type eremeticClient struct {
	httpClient *http.Client
	endpoint   string

    urlCache *xcommon.NameIPCache
}

// New returns a new instance of a Client.
func NewEremeticClient(endpoint string, client *http.Client) (*eremeticClient, error) {
	return &eremeticClient{
		httpClient: client,
		endpoint:   endpoint,
        urlCache: xcommon.NewURLNameIPCache(endpoint, 120),
	}, nil
}

func (c *eremeticClient) Endpoint() string {
    if c.urlCache == nil {
        return c.endpoint
    } else {
        return c.urlCache.GetURL()
    }
}

// AddTask sends a request for a new task to be scheduled.
func (c *eremeticClient) AddTask(r eremetic.Request) (string, error) {
	var buf bytes.Buffer

	err := json.NewEncoder(&buf).Encode(r)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", c.Endpoint() + "/task", &buf)
	if err != nil {
		return "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
    defer resp.Body.Close()

    taskId := ""
    decoder := json.NewDecoder(resp.Body)
    if resp.StatusCode == http.StatusAccepted {
        err = decoder.Decode(&taskId)
        if err != nil {
            return "", err
        } else {
            return taskId, nil
        }
    } else if resp.StatusCode == http.StatusNotFound {
        return "", BE_CONN_ERR
    } else {
        var errDoc errorDocument
        err = decoder.Decode(&errDoc)
        if err != nil {
            return "", err
        }
        if resp.StatusCode == http.StatusInternalServerError {
            err = BE_INTERNAL_ERR
        }
        if resp.StatusCode == http.StatusServiceUnavailable {
            err = BE_QFULL_ERR
        }

        if err == nil {
            err = errors.New("eremetic error " + errDoc.Error)
        }

        return "", err
    }

	return taskId, nil
}

// Task returns a task with a given ID.
func (c *eremeticClient) Task(id string) (*eremetic.Task, error) {
	req, err := http.NewRequest("GET", c.Endpoint()+"/task/"+id, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
    if resp.StatusCode == http.StatusNotFound {
        return nil, BE_TASK_NOT_FOUND
    }

    defer resp.Body.Close()
	var task eremetic.Task
	err = json.NewDecoder(resp.Body).Decode(&task)
	if err != nil {
		return nil, err
	}

	return &task, nil
}

// Tasks returns all current tasks.
func (c *eremeticClient) Tasks() ([]eremetic.Task, error) {
	req, err := http.NewRequest("GET", c.Endpoint()+"/task", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

    defer resp.Body.Close()
	var tasks []eremetic.Task
	err = json.NewDecoder(resp.Body).Decode(&tasks)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

// Sandbox returns a sandbox resource for a given task.
func (c *eremeticClient) Sandbox(taskID, file string, localFilePath string) error {
	u := fmt.Sprintf("%s/task/%s/%s", c.Endpoint(), taskID, file)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return errors.New("fetch sandbox status no ok")
    }

    localFile, err := os.OpenFile(localFilePath, os.O_RDWR|os.O_CREATE, 0755)
    if err != nil {
        return err
    }
    defer localFile.Close()

	_, err = io.Copy(localFile, resp.Body)

	return err
}

// Version returns the version of the Eremetic server.
func (c *eremeticClient) Version() (string, error) {
	u := fmt.Sprintf("%s/version", c.Endpoint())
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}

    defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (c *eremeticClient) Kill(taskID string) error {
	u := fmt.Sprintf("%s/task/%s/kill", c.Endpoint(), taskID)
	req, err := http.NewRequest("POST", u, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
    defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Unexpected status code `%s`",
            resp.Status)
	}

	return nil
}

func (c *eremeticClient) Delete(taskID string) error {
	u := fmt.Sprintf("%s/task/%s", c.Endpoint(), taskID)
	req, err := http.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
    defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Unexpected status code `%s`",
            resp.Status)
	}

	return nil
}

func (c *eremeticClient) RemovableSlaves() (error, []string) {
	u := fmt.Sprintf("%s/Slaves/removable", c.Endpoint())
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err, nil
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err, nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Unexpected status code `%s`",
			resp.Status), nil
	}

	var slaves []string
	err = json.NewDecoder(resp.Body).Decode(&slaves)
	if err != nil {
		return err, nil
	}

	return nil, slaves
}

func (c *eremeticClient) Ping() error {
	u := fmt.Sprintf("%s/ping", c.Endpoint())
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
        return errors.New("ping error")
	}

    return nil
}
