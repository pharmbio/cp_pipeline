#!/usr/bin/env python3



########### HEJ FRAMTIDEN ################
# todo:
# * get channel_map into the database
# * get plate acqusitions into the database
# * fetch db login info from secret
# * fetch only images that have not been analysed from a plate acqusition?
# * store the imgset file as a configmap for each job?
# * fix the job spec yaml, the command and mount paths (root vs user etc)
# * make sure the worker container image exists and works






import psycopg2
import psycopg2.extras
import sys
from kubernetes import client, config
import logging
import re
import yaml
from random import randrange
import base64
import os

# start logging
logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d:%H:%M:%S',
            level=logging.DEBUG)



# function for making a cellprofiler formatted csv file
def make_imgset_csv(imgsets, channel_map):
    
#    # fetch channel map from db
#    logging.debug('Running query.')
#    cursor.execute(f"""
#                        SELECT channel, dye
#                        FROM channel_map
#                        WHERE map_id='{channel_map}'
#                        """)
#    dyes = cursor.fetchall()
#    ch_names = {}
#    for dye in dyes:
#        ch_names[dye['channel']] = dye['dye']
	
    # placeholder instead of a db query
    ch_names = {'1':'HOECHST', '2':'SYTO', '3':'MITO', '4':'CONCAVALIN', '5':'PHALLOIDINandWGA'}

    ### create header row 
    header = ""
   
    for ch_nr,ch_name in sorted(ch_names.items()):
        header += f"FileName_{ch_nr}_{ch_name},"

    header += "Group_Index,Group_Number,ImageNumber,Metadata_Barcode,Metadata_Site,Metadata_Well,"

    for ch_nr,ch_name in sorted(ch_names.items()):
        header += f"PathName_{ch_nr}_{ch_name},"

    for ch_nr,ch_name in sorted(ch_names.items()):
        header += f"URL_{ch_nr}_{ch_name},"

    # remove last comma and add newline
    header = header[:-1]+"\n"
    ###

    # init conent
    content = ""

    # for each imgset
    for imgset_counter,imgset in enumerate(imgsets):

        # construct the csv row
        row = ""

        # sort the images in the imgset by channel id
        sorted_imgset = sorted(imgset, key=lambda k: k['channel'])

        # add filenames
        for img in sorted_imgset:
            img_filename = os.path.basename(img['path'])
            row += f"{img_filename},"

        # add imgset info
        row += f"{imgset_counter},1,{imgset_counter},{img['plate']},{img['site']},{img['well']},"

        # add file paths
        for img in sorted_imgset:
            img_dirname = os.path.dirname(img['path'])
            row += f"{img_dirname},"

        # add file urls
        for img in sorted_imgset:
            row += f"file:{img['path']}"

        # remove last comma and add a newline before adding it to the content
        content += row[:-1] + "\n"

    # return the header and the content
    return f"{header}{content}"






try:

    # load the kube config
    config.load_kube_config()

    # fetch db secret
    # secret = client.CoreV1Api().read_namespaced_secret("postgres-pass", "cppipeline")

    # connect to the db
    logging.info("Connecting to db.")
    connection = None
    connection = psycopg2.connect(  database='imagedb', 
                                    user='postgres', 
                                    host='130.238.44.10', 
                                    port='5432', 
                                    password='example')

    # make results into dicts
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # ask for all new plate acquisitions
#    logging.debug('Running query.')
#    cursor.execute('''
#                        SELECT *
#                        FROM image_analyses_v1
#                        WHERE path LIKE '%Polina%'
#                       ''')
#    acquisitions = cursor.fetchall()

    # placeholder istead of the db query above
    acquisitions = [{'barcode':'A549-24hr-1', 'timepoint':1, "channel_map":1}, 
                    {'barcode':'U2OS-20X-24h-L1', 'timepoint':1, "channel_map":1}, 
                    {'barcode':'WTday7-40X-H2O2-Glu', 'timepoint':1, "channel_map":1}]



    # process each plate
    logging.debug('Looping over query results.')
    for acquisition in acquisitions:

        # fetch all images belonging to the plate acquisition
        logging.debug('Running query.')
        cursor.execute(f"""
                            SELECT *
                            FROM images
                            WHERE plate='{acquisition['barcode']}'
                            AND   timepoint={acquisition['timepoint']} 
                           """) # also NOT IN (select * from images_analysis where analysed=None) or something
        imgs = cursor.fetchall()

        # for img in acquisition
        imgsets = {}
        img_infos = {}
        for img in imgs:

            # readability
            imgset_id = f"{img['well']}-{img['site']}"

            # if it has been seen before
            try:
                imgsets[imgset_id] += [img['path']]
                img_infos[imgset_id] += [img]
            # if it has not been seen before
            except KeyError:
                imgsets[imgset_id] = [img['path']]
                img_infos[imgset_id] = [img]

        # submit each imgset as a job
        for i,imgset_id in enumerate(imgsets):
            #print(imgset)

            # generate cellprofiler imgset file for this imgset
            imgset_file = make_imgset_csv(imgsets=[img_infos[imgset_id]], channel_map=acquisition['channel_map'])

            # create config map with imgset csv
            # client.create_namespaced_configmap()
    
            dep = yaml.safe_load(f"""apiVersion: batch/v1
kind: Job 
metadata:
  name: testpod
  namespace: cppipeline
  labels:
    pod-type: cppipeline
spec:
  template:
    spec:
      containers:
      - name: test
        image: ubuntu:xenial
        command: ["/bin/sh"]
        args: ["-c", "echo hej; sleep 1000; echo done;"]
        resources:
            limits:
              cpu: 1000m
              memory: 1Gi 
            requests:
              cpu: 1000m
              memory: 1Gi 
        volumeMounts:
        - mountPath: /share/mikro
          name: mikroimages
        - mountPath: "/root/.kube/"
          name: kube-config
      restartPolicy: Never
      volumes:
      - name: mikroimages
        persistentVolumeClaim:
          claimName: micro-images-pvc
      - name: kube-config
        secret:
          secretName: cppipipeline-kube-conf                                                                          
          items:
          - key: kube-config
            path: config""")

            k8s_batch_v1 = client.BatchV1Api()
#            print(dep)
            #resp = k8s_batch_v1.create_namespaced_job(
#                    body=dep, namespace="cppipeline")
            #logging.info(f"Deployment created. status='{resp.metadata.name}'")


    











# catch db errors
except (psycopg2.Error) as error:
    logging.error(error)

finally:
    #closing database connectionection.
    if connection:
        cursor.close()
        connection.close()
        logging.info("PostgreSQL connection is closed")

