task CleanEnv {
    File workdir
    
    command {
	rm -f ${workdir}/logs/*
	rm -f ${workdir}/partisaner_db
    }

    output {
	String done = "yes"
    }
}

task PrepareData {
    File src_path
    File dst_path
    String hdfsURI

    command {
	datamover --copy-method copyLocal --source-path ${src_path} --target-path ${dst_path}
    }
    
    output {
	String done = "yes"
    }

    runtime {
        docker: "datamover"
        cpu: "2"
        memory: "10G"
	usextaosparkscheduler: "true"
        SparkExecutorURI: "${hdfsURI}"
    }
}

task MPITask {
    File workdir
    String clean_env
    String args
	
    command {
        cd ${workdir} && xxxpartisaner -t mpich -n mpich -i -p 'cpu=1&mem=1000&workdir=/tmp/&cmd=/home/mpi/mpich-install/run/calculator-PI steps 50000&image=mpich-pipeline&volmap./mnt/alamo-vol1/tensorflow=/tmp&${args}'
    }

    runtime {
        docker: "partisaner"
        cpu: 2
        memory: "1G"
        retry: 1
	usextaoscheduler: "true"
    }

    output {
       String done = "yes"
    }
}   

task TensorflowTrainTask {
    File workdir
    String clean_env
    String calc_steps
    Array[String] prepare_data
    String args
	
    command {
	cd ${workdir} && partisaner -t tensorflow -n tensorflow -i -p 'Roles.0.Name=ps&Roles.0.Cpu=1.0&Roles.0.Mem=1000.0&Roles.1.Name=worker&Roles.1.Cpu=2.0&Roles.1.Mem=2000.0&cmd=sh /tmp/bin/run.sh /tmp/steps&image=tensorflow&Volmap./mnt/alamo-vol1/tensorflow=/tmp&logdir=logs&LogInterval=60&AbortOnFail=true&${args}'
    }

    runtime {
        docker: "partisaner"
        cpu: 2
        memory: "1G"
        retry: 1
	usextaoscheduler: "true"
    }
}   

workflow tensorflow {
    Array[File] datas
    File path
    File tfWorkdir
    File mpiWorkdir
    String tfArgs
    String mpiArgs
    String hdfsURI

    call CleanEnv as CleanTfWorkdir {
	input: workdir=tfWorkdir
    }

    call CleanEnv as CleanMPIWorkdir {
	input: workdir=mpiWorkdir
    }

    scatter(file in datas) {
	call PrepareData {
	    input: src_path=file,dst_path=path,hdfsURI=hdfsURI
	}
    }

    call MPITask {
	input: workdir=mpiWorkdir,clean_env=CleanMPIWorkdir.done,args=mpiArgs
    }
	
    call TensorflowTrainTask {
	input: clean_env=CleanTfWorkdir.done,calc_steps=MPITask.done, prepare_data=PrepareData.done,workdir=tfWorkdir,args=tfArgs
    }
}
