FROM ubuntu:20.04

# update and install
RUN apt update && \
    apt upgrade
RUN apt install -y python3 python3-pip vim

# install python packages
RUN pip3 install psycopg2-binary kubernetes

# create user
RUN useradd -ms /bin/bash user
USER user
WORKDIR /home/user

# add scripts later
COPY cpp_master.py /analyses/



