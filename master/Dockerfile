FROM ubuntu:22.04

# update and install
RUN apt update && \
    apt install -y python3 python3-pip vim ssh

# install python packages
RUN pip3 install psycopg2-binary \
                 kubernetes \
                 pandas==2.2 \
                 pyarrow==16


# set timezone
ENV TZ=Europe/Stockholm
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Create a new group and user 'cpp' with specified UID and GID
RUN groupadd -g 20000 cpp && \
    useradd -m -u 20000 -g cpp -s /bin/bash cpp

# add scripts last
COPY cpp_master.py /cpp/
COPY hpc_utils.py /cpp/
COPY database.py /cpp/