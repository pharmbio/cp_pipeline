#!/bin/bash

set -o pipefail

mkdir -p "$OUTPUT_PATH"

logfile="$OUTPUT_PATH/cp.log"

# output variables as debug info
echo "JOB_TIMEOUT=$JOB_TIMEOUT" | tee -a "$logfile"
echo "PIPELINE_FILE=$PIPELINE_FILE" | tee -a "$logfile"
echo "IMAGESET_FILE=$IMAGESET_FILE" | tee -a "$logfile"
echo "OUTPUT_PATH=$OUTPUT_PATH" | tee -a "$logfile"
echo "NODE_NAME=$NODE_NAME" | tee -a "$logfile"

# launch cellprofiler set timeout because some jobs never finish (analyses on images never converge)
timeout $JOB_TIMEOUT \
nice -n 19 \
cellprofiler \
-r \
-c \
-p $PIPELINE_FILE \
--data-file $IMAGESET_FILE \
-o $OUTPUT_PATH \
--plugins-directory /CellProfiler/plugins 2>&1 | tee -a "$logfile"

# Set exit code to 0 if job was exited due to timeout
exit_code=$?
echo "exit_code=$exit_code" | tee -a "$logfile"
if [ $exit_code -eq 124 ]; then
   echo "JOB_KILLED_BY_TIMEOUT" | tee -a "$logfile"
   exit_code=0
fi

# Write an "error" file if exit code is non-zero
if [ $exit_code -ne 0 ]; then
  touch "$OUTPUT_PATH/error"
else
  # Only write a "finished" file if there is no error
  touch "$OUTPUT_PATH/finished"
fi

exit $exit_code
