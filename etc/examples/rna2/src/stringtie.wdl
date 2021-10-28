task assembly {

  File bam
  File gtf
  String sampleName

  command {
	 stringtie  ${bam} -o  ${sampleName}.gtf -G  ${gtf} -C ${sampleName}.cov  -p 12 -A  ${sampleName}.tab 
  }
  output {
        File out = "${sampleName}.gtf" 
  }
  runtime{
     docker:"mrna:latest"
     cpu:"25"
     memory:"30000MB"
  }
}
task mergegtf {

  Array[File] gtfs
  File gtf
  String name

  command {
	 stringtie --merge  -o  ${name}_merge.gtf -G  ${gtf}    ${sep=' ' gtfs}  
  }
  output {
        File out = "${name}_merge.gtf" 
  }
  runtime{
     docker:"mrna:latest"
     cpu:"25"
     memory:"30000MB"
  }
}

task extract {

  File bam

  command {
	basename ${bam}|cut -d '.' -f 1
  }
  output {
        String value =read_string(stdout())
  }
}

task express {

  File bam
  File gtf
  String sampleName
  File dir

  command {
	 stringtie  ${bam} -G  ${gtf} -C ${sampleName}.express.cov -b  ${dir}/${sampleName} -e  -p 6 -A  ${sampleName}.express.tab -o ${dir}/${sampleName}/${sampleName}.express.gtf
  }
  output {
        File ballgown = "${dir}/${sampleName}" 
	File cov="${sampleName}.express.cov"
  }
  runtime{
     docker:"mrna:latest"
     cpu:"25"
     memory:"30000MB"
  }
}

task exptocsv {

  File dir
  Array[File]  new

  command {
	 prepDE.py -i ${dir}  -g  ${dir}/gene_count_matrix.csv  -t  ${dir}/transcript_count_matrix.csv
  }
  output {
        File count = "${dir}/gene_count_matrix.csv" 
  }
  runtime{
     docker:"mrna:latest"
     cpu:"25"
     memory:"30000MB"
  }
}
task new_trans {

  File fa
  String dir
  File gtf
  command {
	 perl  /Bio/new_transcript.pl -fa  ${fa} -od  ${dir}  -gtf  ${gtf}  
  }
  output {
        File  trans= "${dir}/new.final.transcript.fa" 
        File  newgtf= "${dir}/new.trancript.final.gtf" 
        File  finalgtf= "${dir}/final.all.transcript.gtf" 
  }
  runtime{
     docker:"emboss:latest"
     cpu:"25"
     memory:"30000MB"
  }
}
