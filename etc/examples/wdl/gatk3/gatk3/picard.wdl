task picard_reorder_tool {
    File reference
    File jvmtemp = "/tmp/"
    File inputSamFile

    command {
	java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -Djava.io.tmpdir=${jvmtemp} -jar /bio/gatk/picard.jar ReorderSam I=${inputSamFile} R=${reference} O=output.sam
    }

    output {
        File sam = "output.sam"
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "8G"
    }
}

task picard_sort_tool {
    File inputBamFile

    command {
       java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/picard.jar SortSam I=${inputBamFile} O=output.bam SORT_ORDER=coordinate CREATE_INDEX=true MAX_RECORDS_IN_RAM=150000
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
