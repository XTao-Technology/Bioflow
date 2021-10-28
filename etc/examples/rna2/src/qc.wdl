task fastqc {

  File Fq
  File path
  command {
	 fastqc --extract ${Fq} -o ${path}

  }
  runtime{
     docker:"fastq:latest"
     cpu:"1.5"
     memory:"10000MB"
  
  }
}


task Q30 {

  File Fq
  String side	
  String sampleName

  command {
	 python2.7 /Bio/q30.py  ${Fq} > ${sampleName}_${side}.Q30.stat

  }
  runtime{
     docker:"rendong-rawqc:latest"
     cpu:"1.5"
     memory:"10000MB"
  
  }
}

task QC {

  File Fq
  String side	
  String sampleName

  command {
	/Bio/kseq_fastq_base ${Fq} >${sampleName}_${side}.stat
  }
  runtime{
     docker:"rendong-rawqc:latest"
     cpu:1.5
     memory:"10000MB"
  
  }
}

task mkdir {

  String sampleName

  command {
	mkdir ${sampleName}
  }
  output {
        File path = "${sampleName}"
  }
}







