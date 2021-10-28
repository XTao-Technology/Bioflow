package main

import (
    "os"
    "fmt"
    . "github.com/xtao/bioflow/common"
)

func main() {
    writer := NewZipWriter("value-test", "test.zip", true)
    err := writer.Write()
    if err != nil {
        fmt.Printf("Write zip error: %s\n", err.Error())
        os.Exit(-1)
    }

    extractor := NewZipExtractor("value-copy", "test.zip")
    err = extractor.Extract()
    if err != nil {
        fmt.Printf("Extract zip error: %s\n", err.Error())
    }
}
