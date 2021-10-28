task ReAlignTargetCreator {
    File inputBamFile
    File reference
    File knownSiteFile1
    File knownSiteFile2
    File knownSiteFile3

    command {
        java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/GenomeAnalysisTK.jar -T RealignerTargetCreator -R ${reference} -I ${inputBamFile} -known ${knownSiteFile1} -known ${knownSiteFile2} -known ${knownSiteFile3} -o output.intervals
    }

    output {
        File intervals = "output.intervals"
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "7G"
    }
}

task IndelRealigner {
    File inputBamFile
    File reference
    File knownSiteFile1
    File knownSiteFile2
    File knownSiteFile3
    File intervals

    command {
       java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/GenomeAnalysisTK.jar -T IndelRealigner -R ${reference} -I ${inputBamFile} -known ${knownSiteFile1} -known ${knownSiteFile2} -known ${knownSiteFile3} -targetIntervals ${intervals} -o output.bam --maxReadsInMemory 6000000
    }

    output {
        File bam = "output.bam"
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "9G"
    }
}

task BaseRecalibrator {
    File inputBamFile
    File reference
    File knownSiteFile1
    File knownSiteFile2
    File knownSiteFile3

    command {
      java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/GenomeAnalysisTK.jar -T BaseRecalibrator -R ${reference} -I ${inputBamFile} -knownSites ${knownSiteFile1} -knownSites ${knownSiteFile2} -knownSites ${knownSiteFile3} -o output.recal
    }

    output {
        File recali = "output.recal"
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "9G"
    }
}

task PrintRead {
    File inputBamFile
    File reference
    File recaliFile

    command {
      java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/GenomeAnalysisTK.jar -T PrintReads -I ${inputBamFile} -BQSR ${recaliFile} -R ${reference} -l INFO -o output.bam
    }

    output {
        File bam = "output.bam"
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "9G"
    }
}

task HaplotypeCaller {
    File inputBamFile
    File reference

    command {
      java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/GenomeAnalysisTK.jar -T HaplotypeCaller -R ${reference} -I ${inputBamFile} --emitRefConfidence GVCF -o output.vcf -variant_index_type LINEAR -variant_index_parameter 128000
    }

    output {
        File vcf = "output.vcf"
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "9G"
    }
}

task GenotypeGVCF {
    File vcf
    File reference

    command {
      java -d64 -server -XX:+UseParallelGC -XX:ParallelGCThreads=2 -Xms2g -Xmx12g -jar /bio/gatk/GenomeAnalysisTK.jar -T GenotypeGVCFs -R ${reference} --variant ${vcf} -o output.vcf
    }

    output {
        File vcf = "output.vcf"
    }

    runtime {
        docker: "gatk3"
        cpu: "2"
        memory: "9G"
    }
}
