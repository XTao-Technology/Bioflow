task samview {
    File inputSamFile
    String outputFile = basename(inputSamFile, ".sam") + ".bam"

    command {
         /bio/samtools view -bS ${inputSamFile} -o ${outputFile}
    }

    output {
        File bam = outputFile
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
