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
    File dfsReference
    File hdfsReference
    File siteFile1
    File siteFile2
    File siteFile3
    String SparkMaster
    String SparkExecutorURI
    String hdfsWorkDir
    Boolean alignWithGATK3

    call hdfstool.cleanup_hdfs_dir as PrepareHDFS {
        input: hdfsDir=hdfsWorkDir
    }

    String preparedHDFSDir = PrepareHDFS.hdfsDir

    scatter( reads in readPairs) {
        if (alignWithGATK3) {
		call bwatool.bwa_mem_tool as MapRead {
            		input: threads=10,sample=sample,reads=reads,reference=dfsReference
        	}

		call picardtool.picard_reorder_tool as ReorderSam {
            		input: inputSamFile=MapRead.sam,reference=dfsReference
        	}

        	call samtool.samview as SamToBam {
            		input: inputSamFile=ReorderSam.sam
        	}

        	call picardtool.picard_sort_tool as SortBam {
            		input: inputBamFile=SamToBam.bam
        	}

    		call hdfstool.dfs_to_hdfs as CopyBWABam {
            		input: inputFile=SortBam.bam,
		   	       outputFile=preparedHDFSDir + "/" + basename(SortBam.bam)
    		}
	}

	if (!alignWithGATK3) {
    		call picardtool.prepare_ubam as PrepareUbam {
            		input: sample=sample,readFile1=reads[0],readFile2=reads[1]
    		}

    		call hdfstool.dfs_to_hdfs as CopyUbam {
            		input: inputFile=PrepareUbam.sam,
		   	       outputFile=preparedHDFSDir + "/" + basename(PrepareUbam.sam)
    		}

        	call bwatool.bwa_spark as BwaSpark {
            		input: bwaIndexFile=bwaIndexFile,reference=hdfsReference,
		   	       inputFile=CopyUbam.hdfsFile,
			       outputBamFile=preparedHDFSDir + "/bwa-" + basename(CopyUbam.hdfsFile, ".sam") + ".bam",
		   	       sparkMaster=SparkMaster,SparkExecutorURI=SparkExecutorURI
    		}
        }

	Array[File?] allBamFiles = [CopyBWABam.hdfsFile, BwaSpark.bam]
	File validBamFile = select_first(allBamFiles)

        call gatksparktool.read_pipeline_spark as ReadSpark {
            input: reference=hdfsReference,
		   inputFile=validBamFile,outputBamFile=preparedHDFSDir + "/read-" + basename(validBamFile),
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
            input: inputFile=PrintReadSpark.bam, reference=hdfsReference,
		   sparkMaster=SparkMaster,SparkExecutorURI=SparkExecutorURI
    	}
   }

}
