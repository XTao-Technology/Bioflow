import "bwa.wdl" as bwatool
import "picard.wdl" as picardtool
import "samtools.wdl" as samtool
import "gatktool.wdl" as gatktool
import "hdfs.wdl" as hdfstool
import "gatk4-spark.wdl" as gatksparktool

workflow Gatk4VarationCall {
    String sample
    File bwaIndexFile
    Array[Array[File]] readPairs
    File reference
    File siteFile1
    File siteFile2
    File siteFile3
    String SparkMaster
    String SparkExecutorURI
    String hdfsWorkDir

    call hdfstool.cleanup_hdfs_dir as PrepareHDFS {
        input: hdfsDir=hdfsWorkDir
    }

    String preparedHDFSDir = PrepareHDFS.hdfsDir

    scatter( reads in readPairs) {
    	call picardtool.prepare_ubam as PrepareUbam {
            input: sample=sample,readFile1=reads[0],readFile2=reads[1]
    	}

    	call hdfstool.dfs_to_hdfs as CopySam {
            input: inputFile=PrepareUbam.sam,
		   outputFile=preparedHDFSDir + "/" + basename(PrepareUbam.sam)
    	}

        call bwatool.bwa_spark as BwaSpark {
            input: bwaIndexFile=bwaIndexFile,reference=reference,
		   inputFile=CopySam.hdfsFile,outputBamFile=preparedHDFSDir + "/bwa-" + basename(CopySam.hdfsFile, ".sam") + ".bam",
		   sparkMaster=SparkMaster,SparkExecutorURI=SparkExecutorURI
    	}

        call gatksparktool.read_pipeline_spark as ReadSpark {
            input: reference=reference,
		   inputFile=BwaSpark.bam,outputBamFile=preparedHDFSDir + "/read-" + basename(BwaSpark.bam),
		   sparkMaster=SparkMaster,SparkExecutorURI=SparkExecutorURI,
		   siteFile1=siteFile1, siteFile2=siteFile2,siteFile3=siteFile3
    	}

        call gatksparktool.sort_read_spark as SortReadSpark {
            input: inputFile=ReadSpark.bam,
		   outputBamFile=preparedHDFSDir + "/sortread-" + basename(ReadSpark.bam),
		   sparkMaster=SparkMaster,SparkExecutorURI=SparkExecutorURI
    	}

        call gatksparktool.print_read_spark as PrintReadSpark {
            input: inputFile=SortReadSpark.bam,
		   outputBamFile=preparedHDFSDir + "/print-" + basename(SortReadSpark.bam),
		   sparkMaster=SparkMaster,SparkExecutorURI=SparkExecutorURI
    	}

        call gatksparktool.haplotype_caller_spark as HaplotypeCallerSpark {
            input: inputFile=PrintReadSpark.bam, reference=reference,
		   sparkMaster=SparkMaster,SparkExecutorURI=SparkExecutorURI
    	}
   }

}
