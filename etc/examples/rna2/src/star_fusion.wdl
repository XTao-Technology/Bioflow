task fusion {
  File ctat
  File junction 
  String sampleName
  command {
	 /install/STAR-Fusion_v1.1.0/STAR-Fusion  --genome_lib_dir ${ctat}  -J ${junction}  --output_dir ${sampleName}_Fusion
  }
  runtime{
     docker:"star-fusion:latest"
     cpu:"25"
     memory:"30000MB"
  }
}
