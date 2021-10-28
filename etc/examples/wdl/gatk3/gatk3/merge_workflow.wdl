import "picard.wdl" as picardtool
import "samtools.wdl" as samtool

workflow mergewithindex {
    Array[File] bamFiles


    call picardtool.picard_merge_tool as MergeBams {
        input: inputBamFiles=bamFiles
    }
    
    call samtool.samindex as IndexBam {
        input: inputBamFile=MergeBams.bam
    }

    output {
        File bam = MergeBams.bam
    }
}
