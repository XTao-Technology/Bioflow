package main

import (
    "io/ioutil"
    "os"

    "github.com/ghodss/yaml"
)

func YamlFileToJSON(yamlFile string) (string, error) {
    tmpJsonFile := yamlFile + ".json"
    rawBytes, err := ioutil.ReadFile(yamlFile)
    if err != nil {
        return "", err
    }
    jsonBytes, err := yaml.YAMLToJSON(rawBytes)
    if err != nil {
        return "", err
    }

    err = ioutil.WriteFile(tmpJsonFile, jsonBytes, os.ModePerm)
    return tmpJsonFile, err
}
