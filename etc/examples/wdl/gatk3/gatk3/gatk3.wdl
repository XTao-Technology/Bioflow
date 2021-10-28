import "bwa.wdl" as bwatool
import "picard.wdl" as picardtool
import "samtools.wdl" as samtool
import "gatktool.wdl" as gatktool

workflow gatk {
    String sample
    Array[File] reads
    File reference
    File siteFile1
    File siteFile2
    File siteFile3

    call bwatool.bwa_mem_tool as MapRead {
        input: threads=10,sample=sample,reads=reads,reference=reference
    }

    call picardtool.picard_reorder_tool as ReorderSam {
        input: inputSamFile=MapRead.sam,reference=reference
    }

    call samtool.samview as SamToBam {
        input: inputSamFile=ReorderSam.sam
    }

    call picardtool.picard_sort_tool as SortBam {
        input: inputBamFile=SamToBam.bam
    }

    call samtool.samindex as IndexBam {
        input: inputBamFile=SortBam.bam
    }

    call gatktool.ReAlignTargetCreator as ReAlignTarget {
        input: inputBamFile=SortBam.bam, reference=reference, knownSiteFile1=siteFile1, knownSiteFile2=siteFile2, knownSiteFile3=siteFile3
    }

    call gatktool.IndelRealigner as IndelRealign {
        input: inputBamFile=SortBam.bam, reference=reference, knownSiteFile1=siteFile1, knownSiteFile2=siteFile2, knownSiteFile3=siteFile3, intervals=ReAlignTarget.intervals
    }

    call gatktool.BaseRecalibrator as BaseRecalibrator {
        input: inputBamFile=IndelRealign.bam, reference=reference, knownSiteFile1=siteFile1, knownSiteFile2=siteFile2, knownSiteFile3=siteFile3
    }

    call gatktool.PrintRead as PrintRead {
        input: inputBamFile=IndelRealign.bam, reference=reference, recaliFile=BaseRecalibrator.recali
    }

    call gatktool.HaplotypeCaller as HaplotypeCaller {
        input: inputBamFile=PrintRead.bam, reference=reference
    }

    call gatktool.GenotypeGVCF as GenotypeGVCF {
        input: vcf=HaplotypeCaller.vcf, reference=reference
    }
}
