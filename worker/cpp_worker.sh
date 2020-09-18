#!/bin/bash

# launch cellprofiler 
cellprofiler \
-r \
-c \
-p $PIPELINE_FILE \
--data-file $IMAGESET_FILE \
-o $OUTPUT_PATH







