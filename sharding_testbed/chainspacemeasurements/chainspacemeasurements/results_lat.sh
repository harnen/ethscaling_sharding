#!/bin/bash
calc(){ awk "BEGIN { print $* }"; }

#if [[ $# -eq 0 ]] ; then
#    echo 'File path required'
#   exit 1
#fi
total=0
count=0

for file in results/*_lat.log
do
#	echo "cat ${file} | sed 's/\[//g' | sed 's/\]//g'"
	value=$(cat ${file} | sed 's/\[//g' | sed 's/\]//g')
#	echo "${value} first"
	if [ ! -z "$value" ]
	then
	   total=$(echo $value $total | awk '{print $1 + $2}')
#	   echo $total $count
	   count=$(($count+1))
	else
		echo "Value not set"
	fi
done
echo $total $count
calc $total/$count

