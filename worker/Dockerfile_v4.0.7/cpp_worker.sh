#!/bin/bash

set -o pipefail

mkdir -p "$OUTPUT_PATH"

# output variables as debug info
echo "OUTPUT_PATH=$OUTPUT_PATH" 2>&1 | tee -a "$OUTPUT_PATH/cp.log"
echo "JOB_TIMEOUT=$JOB_TIMEOUT" 2>&1 | tee -a "$OUTPUT_PATH/cp.log"
echo "PIPELINE_FILE=$PIPELINE_FILE" 2>&1 | tee -a "$OUTPUT_PATH/cp.log"
echo "IMAGESET_FILE=$IMAGESET_FILE" 2>&1 | tee -a "$OUTPUT_PATH/cp.log"

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
echo "exit_code=$exit_code" 2>&1 | tee -a "$OUTPUT_PATH/cp.log"
if [ $exit_code -eq 124 ]; then
   echo "JOB_KILLED_BY_TIMEOUT" 2>&1 | tee -a "$OUTPUT_PATH/cp.log"
   exit_code=0
fi
exit $exit_code
