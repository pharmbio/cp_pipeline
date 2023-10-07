FROM cellprofiler/cellprofiler:4.2.5

# install requirements for cellpose
RUN python3.8 -m pip install pytest-runner setuptools-scm

# install cellpose
RUN python3.8 -m pip install cellpose==2.0.5

# create plugins dir
RUN mkdir -p /CellProfiler/plugins

# download the CellPose plugin
RUN cd /CellProfiler/plugins && wget https://raw.githubusercontent.com/CellProfiler/CellProfiler-plugins/master/active_plugins/runcellpose.py 

# Copy pre-built cellpose models
COPY ./models/ /root/.cellpose/models/

RUN apt-get remove -y openjdk-11-jdk-headless
RUN apt-get update -y && apt-get install -y openjdk-17-jdk-headless

# add our cpp_worker script #
COPY ./cpp_worker.sh /
RUN chmod a+x /cpp_worker.sh
