FROM ubuntu:20.04

# update and install
RUN apt update && \
    apt upgrade -y && \
    apt install -y python3 python3-pip vim

# install python packages
RUN pip3 install psycopg2-binary \
                 kubernetes \
                 pandas==1.5 \
                 pyarrow==10

# add scripts later
COPY cpp_master.py /cpp/
