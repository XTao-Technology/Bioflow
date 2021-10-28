task bwa_mem_tool {
    Int threads
    File reference
    Array[File] reads
    String puID = "putest"
    String plID = "ILLUMINA"
    String sample

    command {
        /bio/bwa mem -R "@RG\tID:1\tPL:${plID}\tPU:${puID}\tSM:${sample}"  -k 2 -t ${threads} ${reference} ${sep=' ' reads} > output.sam
    }

    output {
        File sam = "output.sam"
    }

    runtime {
        docker: "gatk3"
        cpu: "${threads}"
        memory: "10G"
        retry: 1
    }
}
