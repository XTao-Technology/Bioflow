task ballgown {
  File outdir
  File dir
  String vs
  Float logfc
  Float fdr  
  Array[File] new
  command {
	 Rscript  /bio/ballgown.r --inputdir  ${dir}  --outdir ${outdir}  --name  ${vs}  --fdr ${fdr} --logfc ${logfc}
  }
  output {
        File diff = "${outdir}/${vs}.ballgown_gene_all.xls"
        File deg = "${outdir}/${vs}.ballgown_gene_deg.xls"
  }
  runtime{
     docker:"ballgown:latest"
     cpu:"25"
     memory:"30000MB"
  }
}
task deseq2 {
  File outdir
  File count
  String vs
  Float logfc
  Float fdr 

  command {
	 Rscript  /bio/DESeq2.r  --count  ${count}  --outdir ${outdir}  --name  ${vs} --fdr ${fdr} --logfc ${logfc} 
  }
  output {
        File diff = "${outdir}/${vs}.DESeq2_all.xls"
        File deg = "${outdir}/${vs}.DESeq2_deg.xls"
  }
  runtime{
     docker:"deseq2:latest"
     cpu:"25"
     memory:"30000MB"
  }
}

task edger {

  File outdir
  File count
  String vs
  Float logfc
  Float fdr 

  command {
	 Rscript  /bio/edgeR.r  --count  ${count}  --outdir ${outdir}  --name  ${vs} --fdr ${fdr} --logfc ${logfc} 
  }
  output {
        File diff = "${outdir}/${vs}.edgeR_all.xls"
        File deg = "${outdir}/${vs}.edgeR_deg.xls"
  }
  runtime{
     docker:"edger:latest"
     cpu:"25"
     memory:"30000MB"
  }
}

task volcano {

  File all
  File outdir
  String vs
  Float logfc
  Float fdr 

  command {
	 Rscript  /script/plot_volcano.r  --input   ${all}  --outdir ${outdir}  --name  ${vs} --fdr ${fdr} --logfc ${logfc} 
  }
  runtime{
     docker:"ggplot2:latest"
     cpu:"25"
     memory:"30000MB"
  }
}