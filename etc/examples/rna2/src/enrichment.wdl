task index {
  File bam

  command {
	 /bio/samtools index  ${bam} ${bam}.bai
  }
  runtime{
     docker:"biotk:latest"
     cpu:"25"
     memory:"30000MB"
  }
}


task sgseq {
  Array[String] name
  Array[File] bam
  File dir
  File gtf

  command {
	 Rscript /Bio/SGSeq.r  --name ${sep=',' name} --bam  ${sep=',' bam} --outdir  ${dir}  --gtf ${gtf}
  }
  runtime{
     docker:"sgseq:latest"
     cpu:"25"
     memory:"100GB"
  }
}
