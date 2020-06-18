#!/bin/bash


# get all arguments
# check arguments
while getopts 'p:i:' flag; do
  case "${flag}" in
	i) images="${OPTARG}" ;;
    p) pipeline="${OPTARG}" ;;                                                                          
    *) print_usage
    ¦  exit 1 ;;
  esac
done


# construct a imgset file


# launch cellprofiler 
cellprofiler \
-r \
-c \
-p $pipeline \
--data-file=/data/imgset.csv
-o /tmp/cp_output/







