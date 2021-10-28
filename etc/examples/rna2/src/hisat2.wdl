task hisat2 {

  File Ref
  File R1
  File R2
  String sampleName
  command {
	 hisat2  -p 16  -x  ${Ref} -1 ${R1} -2 ${R2}  -S ${sampleName}.sam
  }
  output {
        File sam = "${sampleName}.sam"
  }
  runtime{
     docker:"mrna:latest"
     cpu:"25"
     memory:"100000MB"
  }
}

task samtobam {

  File sam

  command {
	 /bio/samtools view --threads 20 -bS ${sam} > ${sam}.bam
  }
  output {
        File bam = "${sam}.bam"
  }
  runtime{
     docker:"biotk:latest"
     cpu:"25"
     memory:"100000MB"
  }
}

task sortbam {

  File bam

  command {
	 /bio/samtools sort -@ 20 ${bam} > ${bam}.sort.bam
  }
  output {
        File out = "${bam}.sort.bam"
  }
  runtime{
     docker:"biotk:latest"
     cpu:"25"
     memory:"100000MB"
  }
}

task star {

  File Ref
  File R1
  File R2
  String sampleName
  command {
	 STAR --genomeDir ${Ref}  --readFilesIn ${R1}  ${R2}  --runThreadN 16 --twopassMode Basic  --outReadsUnmapped None --chimSegmentMin 12 --chimJunctionOverhangMin 12 --alignSJDBoverhangMin 10 --alignMatesGapMax 100000  --alignIntronMax 100000  --chimSegmentReadGapMax 3  --alignSJstitchMismatchNmax 5 -1 5 5   --outSAMstrandField intronMotif   --outFileNamePrefix ${sampleName}.
  }
  output {
        File sam = "${sampleName}.Aligned.out.sam"
        File junction = "${sampleName}.Chimeric.out.junction"
  }
  runtime{
     docker:"star:latest"
     cpu:"25"
     memory:"100000MB"
  }
}

