task dfs_to_hdfs {
    File inputFile
    File outputFile

    command {
	  hdfs dfs -copyFromLocal ${inputFile} ${outputFile}
    }

    output {
        File hdfsFile = outputFile
    }

    runtime {
        docker: "hadoop"
        cpu: "1"
        memory: "8G"
    }
}

task hdfs_to_dfs {
    File inputFile
    File outputFile

    command {
	hdfs dfs -copyToLocal ${inputFile} ${outputFile}
    }

    output {
        File dfsFile = outputFile
    }

    runtime {
        docker: "hadoop"
        cpu: "1"
        memory: "8G"
    }
}

task cleanup_hdfs_dir {
   File hdfsDir

   command {
      hdfs dfs -rm -f -r ${hdfsDir}
      hdfs dfs -mkdir -p ${hdfsDir}
   }
  
   output {
      String hdfsDir = hdfsDir
   }

   runtime {
       docker: "hadoop"
       cpu: "1"
       memory: "8G"
   }
}
