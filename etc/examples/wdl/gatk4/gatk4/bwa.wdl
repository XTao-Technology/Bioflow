task bwa_mem_tool {
    Int threads
    File reference
    Array[File] reads
    String puID = "putest"
    String plID = "ILLUMINA"
    String sample
    String outputFile = basename(reads[0], ".fq.gz") + ".sam"

    command {
        /bio/bwa mem -R "@RG\tID:1\tPL:${plID}\tPU:${puID}\tSM:${sample}"  -k 2 -t ${threads} ${reference} ${sep=' ' reads} > ${outputFile}
    }

    output {
        File sam = outputFile
    }

    runtime {
        docker: "gatk3"
        cpu: "${threads}"
        memory: "10G"
    }
}

task bwa_spark {
    File bwaIndexFile
    File reference
    File inputFile
    File outputBamFile
    String sparkMaster
    String SparkExecutorURI

    command {
       /opt/gatk4/gatk-launch BwaSpark --input ${inputFile} -O ${outputBamFile} --reference ${reference} --bwamemIndexImage hs37d5.bwamemindex --disableSequenceDictionaryValidation true -- --sparkRunner SPARK --sparkMaster ${sparkMaster} --files ${bwaIndexFile} --num-executors 8 --executor-cores 2 --executor-memory 30g --driver-memory 30g --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' --conf 'spark.driver.extraJavaOptions=-XX:hashCode=0' --conf 'spark.cores.max=16' --conf 'spark.local.dir=/var/spark/storage'
    }

    output {
        File bam = outputBamFile
    }

    runtime {
        docker: "gatk4"
        cpu: "2"
        memory: "20G"
	SparkExecutorURI: "${SparkExecutorURI}"
    }
}
