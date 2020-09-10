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
import random 
import base64
import os
import pathlib
import pdb
import json
import string

# start logging
logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d:%H:%M:%S',
            level=logging.DEBUG)



# fetch all dependencies belongin to an analysis and check if they are finished
def all_dependencies_satisfied(analysis):

    # check if there are any dependencies
    if analysis['depends_on_id']:

        dep_unsatified = False

        # unpack the dependency list
        deps = ",".join(map(str, analysis['depends_on_id']))

        # there are dependencies, fetch them from the db
        logging.debug('Fetching analysis dependencies.')
        cursor.execute(f'''
                            SELECT *
                            FROM image_analyses
                            WHERE id IN ({deps})
                           ''')
        dep_analyses = cursor.fetchall()
        
        # check dependencies and return true if they are all finished
        is_all_analyses_finished = check_analyses_finished(dep_analyses)
        return is_all_analyses_finished

    # there are no dependencies, good to go
    else:
        return True



# check if all analyses in a list are finished
def check_analyses_finished(analyses):

        # check all dependencies
        for analysis in analyses:

            # if a dependency is not finished, return false
            if not analysis['finish']:
                return False

        # if they were all finished
        return True



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
#    channel_map = {}
#    for dye in dyes:
#        channel_map[dye['channel']] = dye['dye']
	
    # placeholder instead of a db query
#    channel_map = {1:'HOECHST', 2:'SYTO', 3:'MITO', 4:'CONCAVALIN', 5:'PHALLOIDINandWGA'}

    ### create header row 
    header = ""
   
    for ch_nr,ch_name in sorted(channel_map.items()):
        header += f"FileName_{ch_nr}_{ch_name},"

    header += "Group_Index,Group_Number,ImageNumber,Metadata_Barcode,Metadata_Site,Metadata_Well,"

    for ch_nr,ch_name in sorted(channel_map.items()):
        header += f"PathName_{ch_nr}_{ch_name},"

    for ch_nr,ch_name in sorted(channel_map.items()):
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



def make_job_yaml(pipeline_file, imageset_file, output_path, job_name):

   return yaml.safe_load(f"""

apiVersion: batch/v1
kind: Job 
metadata:
  name: {job_name}
  namespace: cpp
  labels:
    pod-type: cpp
spec:
  template:
    spec:
      containers:
      - name: cpp-worker
        image: pharmbio/cpp_worker:latest
        imagePullPolicy: Always
        command: ["/cpp_worker.sh"]
        env:
        - name: PIPELINE_FILE
          value: {pipeline_file}
        - name: IMAGESET_FILE
          value: {imageset_file}
        - name: OUTPUT_PATH
          value: {output_path}
        resources:
            limits:
              cpu: 1000m
              memory: 8Gi 
            requests:
              cpu: 1000m
              memory: 8Gi 
        volumeMounts:
        - mountPath: /share/mikro
          name: mikroimages
        - mountPath: /root/.kube/
          name: kube-config
        - mountPath: /cpp_work
          name: cpp
      restartPolicy: Never
      volumes:
      - name: mikroimages
        persistentVolumeClaim:
          claimName: micro-images-pvc
      - name: cpp
        persistentVolumeClaim:
          claimName: cpp-pvc
      - name: kube-config
        secret:
          secretName: cpp-user-kube-config
""")


try:
    
    debug = False
    if os.environ.get('DEBUG'):
        debug = True

    # load the kube config
    config.load_kube_config(str(pathlib.Path.home()) + '/.kube/config')

    # fetch db secret
    secret = client.CoreV1Api().read_namespaced_secret("postgres-password", "cpp")
    postgres_password = base64.b64decode(secret.data['password.postgres']).decode().strip()
    postgres_password = "example"

    # fetch db settings
    configmap = client.CoreV1Api().read_namespaced_config_map("cpp-configs", "cpp")
    config = yaml.load(configmap.data['configs.yaml'])


    if debug:
        postgres_password = "example"
        config['postgres']['host'] = "130.238.44.10"

    # connect to the db
    logging.info("Connecting to db.")
    connection = None
    connection = psycopg2.connect(  database=config['postgres']['db'], 
                                    user=config['postgres']['user'],
                                    host=config['postgres']['host'],
                                    port=config['postgres']['port'], 
                                    password=postgres_password)

    # make results into dicts
    cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    # ask for all new analyses
    logging.debug('Running analyses query.')
    cursor.execute('''
                        SELECT *
                        FROM image_analyses
                        WHERE start IS NULL
                       ''')
    analyses = cursor.fetchall()

    # INSERT INTO "image_analyses" ("plate_acquisition_id", "start", "finish", "error", "meta", "depends_on_id", "result")
    # VALUES ('1', '1999-01-08 04:05:06', '1999-01-10 08:42:00', NULL, '{"docker":{"containers":["ccp_master_aethaeh"]}, "cellprofiler":{"pipeline":"/path/to/pipelines/pipeline1.cppipe"}}', NULL, '{"size":"8"}');
    # INSERT INTO "image_analyses" ("plate_acquisition_id", "start", "finish", "error", "meta", "depends_on_id", "result")
    # VALUES ('1', NULL, NULL, NULL, '{"cellprofiler":{"pipeline":"/path/to/pipelines/pipeline2.cppipe", "batching":1}}', '[0]', NULL);
    # INSERT INTO "image_analyses" ("plate_acquisition_id", "start", "finish", "error", "meta", "depends_on_id", "result")
    # VALUES ('2', NULL, NULL, NULL, '{"cellprofiler":{"pipeline":"/path/to/pipelines/pipeline2.cppipe", "batching":1}}', NULL, NULL);
    
#     # placeholder istead of the db query above
#     analyses = [{'id':'0', 'plate_acquisition_id': '1','start':'1999-01-08 04:05:06', 'finish':'1999-01-10 08:42:00', 'error':None, 'meta':'{"docker":{"containers":["ccp_master_aethaeh"]}, "cellprofiler":{"pipeline":"/path/to/pipelines/pipeline1.cppipe"}}', 'depends_on_id':None, 'result':'{"size":"8"}'},
#                 {'id':'1', 'plate_acquisition_id': '1','start':None, 'finish':None, 'error':None, 'meta':'{"cellprofiler":{"pipeline":"/path/to/pipelines/pipeline2.cppipe", "batching":1}}', 'depends_on_id':'[0]', 'result':None},
#                 {'id':'2', 'plate_acquisition_id': '2','start':None, 'finish':None, 'error':None, 'meta':None, 'depends_on_id':None, 'result':None}]





    for analysis in analyses:

        # skip analyiss if there are unmet dependencies
        if not all_dependencies_satisfied(analysis):
            continue



        # fetch the channel map for the acqusition
        logging.debug('Running channel map query.')
        cursor.execute(f'''
                            SELECT *
                            FROM channel_map
                            WHERE map_id=(SELECT channel_map_id
                                          FROM plate_acquisition
                                          WHERE id={analysis['plate_acquisition_id']})
                           ''')
        channel_map_res = cursor.fetchall()
        channel_map = {}
        for channel in channel_map_res:
            channel_map[channel['channel']] = channel['dye']


        # fetch all images belonging to the plate acquisition
        logging.debug('Fetching images belonging to plate acqusition.')
        cursor.execute(f"""
                            SELECT *
                            FROM images_all_view
                            WHERE plate_acquisition_id='{analysis['plate_acquisition_id']}'
                           """) # also NOT IN (select * from images_analysis where analysed=None) or something
        imgs = cursor.fetchall()

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


            # generate names
            random_identifier = ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(8))
            pipeline_file = f"/cpp_work/pipelines/v1.cppipe"
            imageset_file = f"/cpp_work/input/cpp-worker-job-{analysis['id']}-{random_identifier}.csv"
            output_path = f"/cpp_worker/output/cpp-worker-job-{analysis['id']}-{random_identifier}/"
            job_name = f"cpp-worker-job-{analysis['id']}-{random_identifier}"
            job_yaml = make_job_yaml(pipeline_file, imageset_file, output_path, job_name)

            # generate cellprofiler imgset file for this imgset
            imageset_content = make_imgset_csv(imgsets=[img_infos[imgset_id]], channel_map=channel_map)
            with open(imageset_file, 'w') as imageset_csv:
                imageset_csv.write(imageset_content)
            
            k8s_batch_v1 = client.BatchV1Api()
#            print(dep)
            resp = k8s_batch_v1.create_namespaced_job(
                    body=job_yaml, namespace="cpp")
            logging.info(f"Deployment created. status='{resp.metadata.name}'")

            print("exit here")
            break
    











# catch db errors
except (psycopg2.Error) as error:
    logging.error(error)

finally:
    #closing database connectionection.
    if connection:
        cursor.close()
        connection.close()
        logging.info("PostgreSQL connection is closed")

