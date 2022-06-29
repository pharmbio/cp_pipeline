#!/bin/bash

set -o pipefail

# output variables as debug info
echo "JOB_TIMEOUT=$JOB_TIMEOUT"
echo "PIPELINE_FILE=$PIPELINE_FILE"
echo "IMAGESET_FILE=$IMAGESET_FILE"
echo "OUTPUT_PATH=$OUTPUT_PATH"

mkdir -p "$OUTPUT_PATH"

# launch cellprofiler set timeout because some jobs never finish (analyses on images never converge)
timeout $JOB_TIMEOUT \
cellprofiler \
-r \
-c \
-p $PIPELINE_FILE \
--data-file $IMAGESET_FILE \
-o $OUTPUT_PATH \
--plugins-directory /CellProfiler/plugins 2>&1 | tee -a "$OUTPUT_PATH/cp.log"

# Set exit code to 0 if job was exited due to timeout
exit_code=$?
echo "exit_code=$exit_code"
if [ $exit_code -eq 124 ]; then
   echo "JOB_KILLED_BY_TIMEOUT"
   exit_code=0
fi
exit $exit_code
