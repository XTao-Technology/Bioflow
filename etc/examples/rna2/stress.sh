count=500

while
[ $count -ge 0 ]
do
  errmsg=`biocli job submit job_rna_call_workflow.json`
  if [ $? != 0 ]; then
    echo "Fail to submit job $errmsg"
    exit -1
  else
    echo $errmsg
  fi

  count=`expr $count - 1`
done
