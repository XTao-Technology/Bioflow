task prepare_ubam {
    File readFile1
    File readFile2
    String sample
    String puId = "testpu"
    String plId = "ILLUMINA"
    String lbId = "testlib"
    String outputFile = basename(readFile1, ".fq.gz") + ".sam"

    command {
       java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/picard.jar FastqToSam F1=${readFile1} F2=${readFile2} O=${outputFile} SM=${sample} PU=${puId} PL=${plId} LB=${lbId} 
    }

    output {
        File sam = outputFile
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "8G"
    }
}

task picard_reorder_tool {
    File reference
    File jvmtemp = "/tmp/"
    File inputSamFile
    String outputFile = "reorder-" + basename(inputSamFile)

    command {
	java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -Djava.io.tmpdir=${jvmtemp} -jar /bio/gatk/picard.jar ReorderSam I=${inputSamFile} R=${reference} O=${outputFile}
    }

    output {
        File sam = outputFile
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "8G"
    }
}

task picard_sort_tool {
    File inputBamFile
    String outputFile = "sort-" + basename(inputBamFile)

    command {
       java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/picard.jar SortSam I=${inputBamFile} O=${outputFile} SORT_ORDER=coordinate CREATE_INDEX=true MAX_RECORDS_IN_RAM=150000
    }

    output {
        File bam = outputFile
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "8G"
    }
}

task picard_merge_tool {
    Array[File] inputBamFiles
    Array[String] inputPaths = prefix("I=", inputBamFiles)

    command {
       java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/picard.jar MergeSamFiles ${sep=' ' inputPaths} O=output.bam
    }

    output {
        File bam = "output.bam"
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "8G"
    }
}
