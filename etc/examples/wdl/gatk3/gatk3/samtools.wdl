task samview {
    File inputSamFile

    command {
         /bio/samtools view -bS ${inputSamFile} -o output.bam
    }

    output {
        File bam = "output.bam"
    }

    runtime {
        docker: "gatk3"
        cpu: "1.5"
        memory: "7G"
    }
}

task samindex {
    File inputBamFile

    command {
	/bio/samtools index ${inputBamFile}
    }

    output {
	File index = "bai"
    }

    runtime {
        docker: "gatk3"
        cpu: "1"
        memory: "6G"
    }
}
