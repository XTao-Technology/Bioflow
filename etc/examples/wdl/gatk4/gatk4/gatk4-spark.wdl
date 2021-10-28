task read_pipeline_spark {
    File reference
    File inputFile
    File outputBamFile
    File siteFile1
    File siteFile2
    File siteFile3
    String sparkMaster
    String SparkExecutorURI

    command {
      /opt/gatk4/gatk-launch ReadsPipelineSpark --input ${inputFile} -O ${outputBamFile} --reference ${reference} --knownSites ${siteFile1} --knownSites ${siteFile2} --knownSites ${siteFile3} -- --sparkRunner SPARK --sparkMaster ${sparkMaster} --num-executors 8 --executor-cores 2 --executor-memory 90g --driver-memory 30g --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' --conf 'spark.driver.extraJavaOptions=-XX:hashCode=0' --conf 'spark.cores.max=16' --conf 'spark.local.dir=/var/spark/storage'
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

task sort_read_spark {
    File inputFile
    File outputBamFile
    String sparkMaster
    String SparkExecutorURI

    command {
      /opt/gatk4/gatk-launch SortReadFileSpark -I ${inputFile} -O ${outputBamFile} -- --sparkRunner SPARK --sparkMaster ${sparkMaster} --num-executors 4 --executor-cores 2 --executor-memory 60g --driver-memory 25g --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' --conf 'spark.driver.extraJavaOptions=-XX:hashCode=0' --conf 'spark.cores.max=16' --conf 'spark.local.dir=/var/spark/storage'
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

task print_read_spark {
    File inputFile
    File outputBamFile
    String sparkMaster
    String SparkExecutorURI

    command {
      /opt/gatk4/gatk-launch PrintReadsSpark -I ${inputFile} -O ${outputBamFile} -- --sparkRunner SPARK --sparkMaster ${sparkMaster} --num-executors 8 --executor-cores 2 --executor-memory 20g --driver-memory 4g --conf 'spark.cores.max=16' --conf 'spark.local.dir=/var/spark/storage'
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

task haplotype_caller_spark {
    File inputFile
    File reference
    String sparkMaster
    String SparkExecutorURI

    command {
      /opt/gatk4/gatk-launch HaplotypeCallerSpark --input ${inputFile} --output "output.vcf" --reference ${reference} --emitRefConfidence GVCF -- --sparkRunner SPARK --sparkMaster ${sparkMaster} --num-executors 8 --executor-cores 2 --executor-memory 80g --driver-memory 30g --conf 'spark.executor.extraJavaOptions=-XX:hashCode=0' --conf 'spark.driver.extraJavaOptions=-XX:hashCode=0' --conf 'spark.kryoserializer.buffer.max=2047m' --conf 'spark.cores.max=16' --conf 'spark.local.dir=/var/spark/storage'
    }

    output {
        File vcf = "output.vcf"
    }

    runtime {
        docker: "gatk4"
        cpu: "2"
        memory: "20G"
	SparkExecutorURI: "${SparkExecutorURI}"
    }
}
