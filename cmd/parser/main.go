package main

import (
    "fmt"
    . "github.com/xtao/bioflow/blparser"
    . "github.com/xtao/bioflow/common"
)

func main() {
    /*init the log*/
    config := LoggerConfig{
                Logfile: "./parser.log",
            }
    LoggerInit(&config)

    parser := NewLPHParser()

    test_str1 := "/bio/bwa mem -R '@RG\\tID:1\\tPL:${PLId}\\tPU:${branch.name}\\tSM:${branch.sample}' -k 2 -t 4 $file.Ref $files.* > $output.sam"
    fmt.Println(test_str1)

    biuldCMDInfo := parser.TranslatePlaceHolders(test_str1,
       func (stmt BLStatement) (error, bool) {
            fmt.Println(stmt)

            return nil, true
       })
    fmt.Println(biuldCMDInfo.ErrType)

    test_str2 := "java -Xmx12g -jar /bio/gatk/picard.jar AddOrReplaceReadGroups I=$input.bam O=$output.bam ID=$ReadsId LB=$LBId PL=$PLId PU=$PUId SM=$SMId"
    biuldCMDInfo = parser.TranslatePlaceHolders(test_str2,
       func (stmt BLStatement) (error, bool) {
            fmt.Println(stmt)
            return nil, true
       })
    fmt.Println(biuldCMDInfo.ErrType)

    test_str3 := "ls $input1.sam > $output.ls"
    biuldCMDInfo = parser.TranslatePlaceHolders(test_str3,
       func (stmt BLStatement) (error, bool) {
            fmt.Println(stmt)
            return nil, true
       })
    fmt.Println(biuldCMDInfo.ErrType)

    test_str4 := "exec java $inputs.sam.splitjoinprefix{'-I '} > $output.ls"
    biuldCMDInfo = parser.TranslatePlaceHolders(test_str4,
       func (stmt BLStatement) (error, bool) {
            fmt.Println(stmt)
            return nil, true
       })
    fmt.Println(biuldCMDInfo.ErrType)

    test_str5 := "python /gene/preMap.py -c ${input.cancerbam} -n ${input.normalbam} -m ${output.map} --oc ${outputpath.cbam} --on ${outputpath.nbam}"
    biuldCMDInfo = parser.TranslatePlaceHolders(test_str5,
        func (stmt BLStatement) (error, bool) {
            fmt.Println(stmt)
            return nil, true
        })

    fmt.Println(biuldCMDInfo.ErrType)

    test_str6 := "python /gene/preMap.py -c ${inputdir.cancerbam} -n ${inputdir.normalbam} -m ${outputdirpath.map} --oc ${outputdirpath." +
        "cbam} --on ${outputdir.nbam}"
    biuldCMDInfo = parser.TranslatePlaceHolders(test_str6,
        func (stmt BLStatement) (error, bool) {
            fmt.Println(stmt)
            return nil, true
        })

    fmt.Println(biuldCMDInfo.ErrType)
}
