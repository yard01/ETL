#!/bin/bash

JobName=$1
echo $JobName

if [[ -z $JobName ]]
   then
      echo "Job name is empty!Stopping..."
      exit 1
fi
 
JobNo=`$DSHOME/bin/uvsh "SELECT JOBNO FROM DS_JOBS WHERE NAME = '$JobName';" | tr '\n' ';' | cut -f 3 -d ";"`

if [[ -z $JobNo ]]
   then
      echo "Job number is incorrect!Stopping..."
      exit 1
fi

echo $JobNo

$DSHOME/bin/clear.file RT_LOG$JobNo
$DSHOME/bin/clear.file RT_STATUS$JobNo

exit 0

