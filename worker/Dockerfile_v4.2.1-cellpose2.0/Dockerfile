FROM cellprofiler/cellprofiler:4.2.1

# install requirements for cellpose
RUN python3.8 -m pip install pytest-runner setuptools-scm

# install cellpose
RUN python3.8 -m pip install cellpose==2.0.5

# create plugins dir
RUN mkdir -p /CellProfiler/plugins

# download the CellPose plugin
RUN cd /CellProfiler/plugins && wget https://raw.githubusercontent.com/CellProfiler/CellProfiler-plugins/master/runcellpose.py

# Copy pre-built cellpose models
COPY ./models/ /root/.cellpose/models/

# add our cpp_worker script #
COPY ./cpp_worker.sh /
RUN chmod a+x /cpp_worker.sh




