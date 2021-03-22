#!/bin/bash

# output variables as debug info
echo "JOB_TIMEOUT=$JOB_TIMEOUT"
echo "PIPELINE_FILE=$PIPELINE_FILE"
echo "IMAGESET_FILE=$IMAGESET_FILE"
echo "OUTPUT_PATH=$OUTPUT_PATH"

# launch cellprofiler set timeout because some jobs never finish (analyses on images never converge)
timeout $JOB_TIMEOUT \
cellprofiler \
-r \
-c \
-p $PIPELINE_FILE \
--data-file $IMAGESET_FILE \
-o $OUTPUT_PATH

# Set exit code to 0 if job was exited due to timeout
exit_code=$?
if [ $exit_code -eq 124 ]; then
   exit_code=0
fi
exit $exit_code



