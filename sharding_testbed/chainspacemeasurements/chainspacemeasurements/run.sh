#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'File path required'
    exit 1
fi
for file in $1/*
do
    #whatever you need with "$file"
    fname=$(basename $file)
    fbname=${fname%.txt}
    python tester.py sharding_measurements 3 3 5 1 $file results/${fbname}_tps.log results/${fbname}_lat.log
done
