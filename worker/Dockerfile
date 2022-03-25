FROM cellprofiler/cellprofiler:4.2.1

# install requirements for cellpose
RUN python3.8 -m pip install pytest-runner setuptools-scm

# install cellpose
RUN python3.8 -m pip install cellpose==0.6.5

# create plugins dir
RUN mkdir -p /CellProfiler/plugins

# download the CellPose plugin
RUN cd /CellProfiler/plugins && wget https://raw.githubusercontent.com/CellProfiler/CellProfiler-plugins/62d46ba2247fe0b1be534ebd5bed33f5bcdeb5d3/runcellpose.py

# trigger download of pre-built cellpose models (by starting cellpose program)
RUN python3.8 -m cellpose

# add our cpp_worker script #
COPY cpp_worker.sh /
RUN chmod a+x /cpp_worker.sh




