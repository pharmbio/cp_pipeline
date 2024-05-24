FROM cellprofiler/cellprofiler:4.2.5

RUN apt-get remove -y openjdk-11-jdk-headless
RUN apt-get update -y && apt-get install -y openjdk-17-jdk-headless
RUN apt-get install -y rsync

# Create a new group and user 'cpp' with specified UID and GID
RUN groupadd -g 20000 cpp && \
    useradd -m -u 20000 -g cpp -s /bin/bash cpp

# install requirements for cellpose
RUN python3.8 -m pip install pytest-runner setuptools-scm

# install cellpose
RUN python3.8 -m pip install cellpose==2.0.5

# create plugins dir
RUN mkdir -p /CellProfiler/plugins

# download the CellPose plugin
#RUN cd /CellProfiler/plugins && wget https://raw.githubusercontent.com/CellProfiler/CellProfiler-plugins/master/active_plugins/runcellpose.py
RUN cd /CellProfiler/plugins && wget https://raw.githubusercontent.com/CellProfiler/CellProfiler-plugins/7bfb39005da6717595b9349f320397b4e2ca50ba/active_plugins/runcellpose.py

# Copy pre-built cellpose models
COPY ./models/ /home/cpp/.cellpose/models/

# add our cpp_worker script #
COPY ./cpp_worker.sh /
RUN chmod a+x /cpp_worker.sh
