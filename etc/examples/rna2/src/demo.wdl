import "qc.wdl" as qctool
import "hisat2.wdl" as aligntool
import "stringtie.wdl" as expresstool
import "different.wdl" as difftool
import "Splice_event.wdl" as splicetool
import "star_fusion.wdl" as fusiontool
import "gatk.wdl" as snptool
import "enrichment.wdl" as enrichtool

workflow RNAflow {

  Array[Array[File]] readPairs
  File Ref 
  File ctat 
  File fa
  File gtf
  File fai
  File dict

  Array[String] vs
  Float logfc
  Float fdr
  Float bcv

 
  scatter (sample in readPairs) {
    call qctool.QC  as fq1qc {
      input: Fq=sample[1], side="R1",sampleName=sample[0]
    }
    call qctool.QC  as fq2qc  {
      input: Fq=sample[2], side="R2", sampleName=sample[0]
    }
    call qctool.Q30  as fq1q30 {
      input: Fq=sample[1], side="R1", sampleName=sample[0]
    }
    call qctool.Q30  as fq2q30  {
      input: Fq=sample[2], side="R2", sampleName=sample[0]
    }   
    call qctool.mkdir as mksample {
      input: sampleName=sample[0]
    }    
    call qctool.fastqc  as qc1 {
      input: Fq=sample[1],path=mksample.path
    }
    call qctool.fastqc  as qc2  {
      input: Fq=sample[2],path=mksample.path
    }
    call aligntool.star  {
      input: R1=sample[1],R2=sample[2],Ref=Ref,sampleName=sample[0]
    }
    call aligntool.samtobam  {
      input: sam=star.sam
    }  
    call aligntool.sortbam  {
      input: bam=samtobam.bam
    }     
    call expresstool.assembly  {
      input: bam=sortbam.out,gtf=gtf,sampleName=sample[0]
    }


    call fusiontool.fusion  {
      input: ctat=ctat,junction=star.junction,sampleName=sample[0]
    }


    call snptool.add  {
      input: sam=star.sam,sampleName=sample[0]
    }
    call snptool.MarkDuplicates  {
      input: bam=add.bam,sampleName=sample[0]
    }
    call snptool.SplitNCigarReads {
      input: bam=MarkDuplicates.output_bam,
      bam_index = MarkDuplicates.output_bam_index,
      sampleName=sample[0],ref_fasta=fa,ref_fasta_index=fai,ref_dict=dict
    }
    call snptool.HaplotypeCaller {
      input: bam=SplitNCigarReads.output_bam,
      bam_index = SplitNCigarReads.output_bam_index,
      sampleName=sample[0],ref_fasta=fa,ref_fasta_index=fai,ref_dict=dict
    }
    call snptool.VariantFiltration {
      input: input_vcf=HaplotypeCaller.output_gvcf,
      input_vcf_index = HaplotypeCaller.output_gvcf_index,
      sampleName=sample[0],ref_fasta=fa,ref_fasta_index=fai,ref_dict=dict
    }
    call snptool.bgzip {
      input: input_vcf=VariantFiltration.output_vcf,sampleName=sample[0]
    }
    call snptool.tbi {
      input: input_vcf=bgzip.output_vcf
    }
 }


  call expresstool.mergegtf  {
      input: gtfs=assembly.out,gtf=gtf,name="AllSample"
  }
  call expresstool.new_trans  {
      input: fa=fa,gtf=mergegtf.out,dir="new_transcript"
  }

  call qctool.mkdir as ballgowndir {
      input: sampleName="ballgown"
  }
  
  Array[File] sortbams =sortbam.out
  
  scatter (bam in sortbams) {
	  call expresstool.extract {
	      input: bam=bam
	  }  
	  call expresstool.express  {
	      input: bam=bam,gtf=new_trans.finalgtf,dir=ballgowndir.path,sampleName=extract.value
	  }
	  call splicetool.index  {
	      input: bam=bam
	  }
  }
  
  call expresstool.exptocsv  {
      input: dir=ballgowndir.path,new=express.cov
  }

  call qctool.mkdir as diffdir {
      input: sampleName="Deg_analysis"
  }
  scatter (group in vs) {
	  call difftool.ballgown  {
	      input: outdir=diffdir.path,dir=ballgowndir.path,vs=group,logfc=logfc,fdr=fdr,new=express.cov
	  }
	  call difftool.deseq2  {
	      input: outdir=diffdir.path,count=exptocsv.count,vs=group,logfc=logfc,fdr=fdr
	  }
	  call difftool.edger  {
	      input: outdir=diffdir.path,count=exptocsv.count,vs=group,logfc=logfc,fdr=fdr
	  }
	  call difftool.volcano as bvol  {
	      input: outdir=diffdir.path,all=ballgown.diff,vs=group,logfc=logfc,fdr=fdr
	  }
	  call difftool.volcano as dvol  {
	      input: outdir=diffdir.path,all=deseq2.diff,vs=group,logfc=logfc,fdr=fdr
	  }
	  call difftool.volcano as evol  {
	      input: outdir=diffdir.path,all=edger.diff,vs=group,logfc=logfc,fdr=fdr
	  }
 }

  call qctool.mkdir as splicedir {
      input: sampleName="Splice_event"
  }
  call splicetool.sgseq {
      input: name=extract.value,bam=sortbams,dir=splicedir.path,gtf=gtf
  }

  call snptool.vcfmerge {
      input: input_vcfs=bgzip.output_vcf, input_vcfs_indexes=tbi.output_tbi
  }

}








