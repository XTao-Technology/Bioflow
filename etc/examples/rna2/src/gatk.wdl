task add {

  File sam
  String sampleName

  command {
	/gatk/gatk  AddOrReplaceReadGroups -I ${sam} -O ${sampleName}.added_sorted.bam -SO coordinate --RGID id --RGLB library --RGPL illumina --RGPU machine --RGSM ${sampleName}
  }
  output {
        File bam = "${sampleName}.added_sorted.bam"
  }
  runtime{
     docker:"gatk4/wd:latest"
     cpu:"25"
     memory:"30000MB"
  }
}


task MarkDuplicates {

   File bam
   String sampleName
 	command {
 	    /gatk/gatk  MarkDuplicates --INPUT ${bam} --OUTPUT ${sampleName}.dedupped.bam   --CREATE_INDEX true    --VALIDATION_STRINGENCY SILENT --METRICS_FILE ${sampleName}.metrics
 	}

 	output {
 		File output_bam = "${sampleName}.dedupped.bam"
 		File output_bam_index = "${sampleName}.dedupped.bai"
 		File metrics_file = "${sampleName}.metrics"
 	}

	runtime{
	     docker:"gatk4/wd:latest"
	     cpu:"25"
	     memory:"30000MB"
	}
}


task SplitNCigarReads {

	File bam
	File bam_index
	String sampleName
	File ref_fasta
	File ref_fasta_index
	File ref_dict

	command {
		java -jar /bio/gatk3.7/GenomeAnalysisTK.jar  -T SplitNCigarReads  -R ${ref_fasta}  -I ${bam} 		-o ${sampleName}.split.bam  -rf ReassignOneMappingQuality  -RMQF 255 -RMQT 60  -U ALLOW_N_CIGAR_READS
	}

 	output {
 		File output_bam = "${sampleName}.split.bam"
 		File output_bam_index = "${sampleName}.split.bai"
 	}

	runtime{
	     docker:"biotk/wd:latest"
	     cpu:"25"
	     memory:"30000MB"
	}
}


task BaseRecalibrator {

    File bam
    File bam_index
    String sampleName

    File dbSNP_vcf
    File dbSNP_vcf_index
    Array[File] known_indels_sites_VCFs
    Array[File] known_indels_sites_indices

    File ref_dict
    File ref_fasta
    File ref_fasta_index

    command {
        /gatk/gatk  --java-options "-XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=10 -XX:+PrintFlagsFinal             -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintGCDetails   -Xloggc:gc_log.log -Xms4000m"         BaseRecalibrator  -R ${ref_fasta}   -I ${bam}   --use-original-qualities    -O ${sampleName}.recal.report -known-sites ${dbSNP_vcf}  -known-sites ${sep=" --known-sites " known_indels_sites_VCFs}
    }

	output {
		File recalibration_report = "${sampleName}.recal.report"
	}

	runtime{
	     docker:"gatk4/wd:latest"
	     cpu:"25"
	     memory:"30000MB"
	}
}


task ApplyBQSR {

    File bam
    File bam_index
    String sampleName
    File recalibration_report

    File ref_dict
    File ref_fasta
    File ref_fasta_index

    command {
        /gatk/gatk  --java-options "-XX:+PrintFlagsFinal -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps             -XX:+PrintGCDetails -Xloggc:gc_log.log  -XX:GCTimeLimit=50 -XX:GCHeapFreeLimit=10 -Xms3000m"              ApplyBQSR  --add-output-sam-program-record    -R ${ref_fasta}   -I ${bam}   --use-original-qualities      -O ${sampleName}.recal.bam   --bqsr-recal-file ${recalibration_report}
    }

    output {
        File output_bam = "${sampleName}.recal.bam"
        File output_bam_index = "${sampleName}.recal.bai"
    }

    runtime{
	     docker:"gatk4/wd:latest"
	     cpu:"25"
	     memory:"30000MB"
    }
}

task HaplotypeCaller {

	File bam
	File bam_index
	String sampleName

  	File ref_dict
  	File ref_fasta
  	File ref_fasta_index

	command {
		java -jar /bio/gatk3.7/GenomeAnalysisTK.jar   -T HaplotypeCaller   -R ${ref_fasta}  -I ${bam} 		    -dontUseSoftClippedBases   -stand_call_conf 20.0   -o ${sampleName}.vcf.gz
	}

    output {
        File output_gvcf = "${sampleName}.vcf.gz"
        File output_gvcf_index = "${sampleName}.vcf.gz.tbi"
    }

    runtime{
	     docker:"biotk/wd:latest"
	     cpu:"25"
	     memory:"30000MB"
    }
}

task VariantFiltration {

	File input_vcf
	File input_vcf_index
	String sampleName

  	File ref_dict
  	File ref_fasta
  	File ref_fasta_index

	command {
		 /gatk/gatk   VariantFiltration --R ${ref_fasta} --V ${input_vcf} --window 35 			--cluster 3 	--filter-name "FS" --filter "FS > 30.0" --filter-name "QD" 			--filter "QD < 2.0" -O ${sampleName}.filtered.vcf
	}

	output {
    	File output_vcf = "${sampleName}.filtered.vcf"
    	File output_vcf_index = "${sampleName}.filtered.vcf.tbi"
	}

	runtime{
		     docker:"gatk4/wd:latest"
		     cpu:"25"
		     memory:"30000MB"
	}
}

task MergeVCFs {
    Array[File] input_vcfs
    Array[File] input_vcfs_indexes

    command {
        /gatk/gatk   --java-options "-Xms2000m"    MergeVcfs  --INPUT ${sep=' --INPUT=' input_vcfs}             --OUTPUT All_sample.merge.vcf
    }

    output {
        File output_vcf = "All_sample.merge.vcf"
        File output_vcf_index = "All_sample.merge.vcf.tbi"
    }


    runtime{
		     docker:"gatk4/wd:latest"
		     cpu:"25"
		     memory:"30000MB"
    }
}


task vcfmerge{

    Array[File] input_vcfs
    Array[File] input_vcfs_indexes

    command {
        vcf-merge  ${sep=' ' input_vcfs}  >All_sample.merge.vcf
    }

    output {
        File output_vcf = "All_sample.merge.vcf"
    }


    runtime{
		     docker:"vcftools:v0.1"
		     cpu:"8"
		     memory:"30000MB"
    }
}

task bgzip  {

	File input_vcf
	String sampleName

	command {
		 /BioBin/htslib/bgzip -c ${input_vcf} > ${sampleName}.filtered.vcf.gz 	
	}

	output {
    	File output_vcf = "${sampleName}.filtered.vcf.gz"
	}

	runtime{
		     docker:"bwa:base"
		     cpu:"25"
		     memory:"30000MB"
	}
}

task tbi  {

	File input_vcf

	command {
		 /BioBin/htslib/tabix  -p vcf  ${input_vcf} 	
	}

	output {
    	File output_tbi = "${input_vcf}.tbi"
	}

	runtime{
		     docker:"bwa:base"
		     cpu:"25"
		     memory:"30000MB"
	}
}