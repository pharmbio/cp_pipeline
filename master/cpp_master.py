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
# * build csv straight to file to reduce memory problems, need at least 32MB at moment



import psycopg2
import psycopg2.extras
import sys
import kubernetes
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
import itertools
import math
import pathlib
import csv
import shutil
import datetime
import time

# divide a dict into smaller dicts with a set number of items in each
def chunk_dict(data, chunk_size=1):

    # create iterator of the dict
    it = iter(data)

    # for each step
    for i in range(0, len(data), chunk_size):

        # produce a dict with chunk_size items in it
        yield {k:data[k] for k in itertools.islice(it, chunk_size)}



# fetch all dependencies belongin to an analysis and check if they are finished
def all_dependencies_satisfied(analysis, cursor):

    # check if there are any dependencies
    if analysis['depends_on_sub_id']:

        dep_unsatified = False

        # unpack the dependency list
        # turn json list into a comma separated text of values as SQL wants it
        deps = ",".join(map(str, analysis['depends_on_sub_id']))

        # there are dependencies, fetch them from the db
        logging.debug('Fetching analysis dependencies.')
        cursor.execute(f'''
                            SELECT *
                            FROM image_sub_analyses
                            WHERE sub_id IN ({deps})
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

        logging.info("Inside check_analyses_finished")

        # check all dependencies
        for analysis in analyses:

            # if a dependency is not finished, return false
            if not analysis['finish']:
                return False

        # if they were all finished
        return True


# function for making a cellprofiler formatted csv file
def make_imgset_csv(imgsets, channel_map, storage_paths, use_icf):
    
#    # fetch channel map from db
#    logging.info('Running query.')
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
        header += f"FileName_{ch_name}," #header += f"FileName_w{ch_nr}_{ch_name},"

    header += "Group_Index,Group_Number,ImageNumber,Metadata_Barcode,Metadata_Site,Metadata_Well,"

    for ch_nr,ch_name in sorted(channel_map.items()):
        header += f"PathName_{ch_name},"

    for ch_nr,ch_name in sorted(channel_map.items()):
        header += f"URL_{ch_name},"

    # Add Illumination correction headers if needed
    if use_icf:
        # First as URL_
        for ch_nr,ch_name in sorted(channel_map.items()):
            header += f"URL_ICF_{ch_name},"

        # And then as PathName_
        for ch_nr,ch_name in sorted(channel_map.items()):
            header += f"PathName_ICF_{ch_name},"
        
         # And then as FileName_
        for ch_nr,ch_name in sorted(channel_map.items()):
            header += f"FileName_ICF_{ch_name},"

    # remove last comma and add newline
    header = header[:-1]+"\n"
    ###

    # init counter
    content = ""

    # for each imgset
    for imgset_counter,imgset in enumerate(imgsets.values()):
        #pdb.set_trace()
        # construct the csv row
        row = ""

        # sort the images in the imgset by channel id
        sorted_imgset = sorted(imgset, key=lambda k: k['channel'])

        # add filenames
        for img in sorted_imgset:
            img_filename = os.path.basename(img['path'])
            row += f"{img_filename},"

        # add imgset info
        row += f"{imgset_counter},1,{imgset_counter},{img['plate_barcode']},{img['site']},{img['well']},"

        # add file paths
        for img in sorted_imgset:
            img_dirname = os.path.dirname(img['path'])
            row += f"{img_dirname},"

        # add file urls
        for img in sorted_imgset:
            row += f"file:{img['path']},"

        # add illumination file names, both as URL_ and PATH_ - these are not uniqe per image,
        # all images with same channel have the same correction image
        if use_icf:
            # First as URL
            for ch_nr,ch_name in sorted(channel_map.items()):
                row +=  f"file:{storage_paths['full']}/ICF_{ch_name}.npy,"
            
            # Also as PathName_
            for ch_nr,ch_name in sorted(channel_map.items()):
                row +=  f"{storage_paths['full']},"

            # Also as FileName_
            for ch_nr,ch_name in sorted(channel_map.items()):
                row +=  f"ICF_{ch_name}.npy,"

        # remove last comma and add a newline before adding it to the content
        content += row[:-1] + "\n"

    # return the header and the content
    return f"{header}{content}"



def  make_jupyter_yaml(notebook_file, output_path, job_name, analysis_id, sub_analysis_id, analyis_input_folder, analysis_input_file):

    docker_image="pharmbio/pharmbio-notebook:tf-2.1.0"
    
    # docker run -e WORK_FOLDER="katt" -it -u root -v /share/data/cellprofiler/automation/:/cpp_work/ pharmbio/pharmbio-notebook:tf-2.1.0 jupyter nbconvert --to pdf --output=/cpp_work/notebooks/hello.output.ipynb.pdf /cpp_work/notebooks/hello.ipynb

    return yaml.safe_load(f"""

apiVersion: batch/v1
kind: Job 
metadata:
  name: {job_name}
  namespace: {get_namespace()}
  labels:
    pod-type: cpp
    app: cpp-worker
    analysis_id: "{analysis_id}"
    sub_analysis_id: "{sub_analysis_id}"
spec:
  backoffLimit: 0
  template:
    spec:
      containers:
      - name: cpp-worker
        image: {docker_image}
        imagePullPolicy: Always
        #command: ["sleep", "3600"]
        command: ["/bin/sh", "-c"]
        args:
        - >
          jupyter nbconvert --to notebook --inplace --execute --ExecutePreprocessor.timeout=600 --output-dir {output_path} {notebook_file} &&
          jupyter nbconvert --to pdf --TemplateExporter.exclude_input=True --no-prompt --output-dir {output_path} {notebook_file}
        env:
        - name: ANALYSIS_INPUT_FILE
          value: {analysis_input_file}
        - name: ANALYSIS_INPUT_FOLDER
          value: {analyis_input_folder}
        resources:
            limits:
              cpu: 2000m
              memory: 4Gi 
            requests:
              cpu: 500m
              memory: 2Gi 
        volumeMounts:
        - mountPath: /share/mikro/IMX/MDC_pharmbio/
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





def make_cellprofiler_yaml(cellprofiler_version, pipeline_file, imageset_file, output_path, job_name, analysis_id, sub_analysis_id, job_timeout):

    if is_debug():
       docker_image="ghcr.io/pharmbio/cpp_worker:" + cellprofiler_version + "-latest"
    else:
       docker_image="ghcr.io/pharmbio/cpp_worker:" + cellprofiler_version + "-stable"

    return yaml.safe_load(f"""

apiVersion: batch/v1
kind: Job 
metadata:
  name: {job_name}
  namespace: {get_namespace()}
  labels:
    pod-type: cpp
    app: cpp-worker
    analysis_id: "{analysis_id}"
    sub_analysis_id: "{sub_analysis_id}"
spec:
  backoffLimit: 10
  template:
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
            - matchExpressions:
              - key: kubernetes.io/hostname
                operator: In
                values:
                - brolin
                #- klose-vm-worker
                #- limpar
                #- messi-vm-worker
      containers:
      - name: cpp-worker
        image: {docker_image}
        imagePullPolicy: Always
        #command: ["sleep", "3600"]
        command: ["/cpp_worker.sh"]
        env:
        - name: PIPELINE_FILE
          value: {pipeline_file}
        - name: IMAGESET_FILE
          value: {imageset_file}
        - name: OUTPUT_PATH
          value: {output_path}
        - name: JOB_TIMEOUT
          value: "{job_timeout}"
        resources:
            limits:
              cpu: 1000m
              memory: 8Gi 
            requests:
              cpu: 300m
              memory: 2Gi 
        volumeMounts:
        - mountPath: /share/mikro/IMX/MDC_pharmbio/
          name: mikroimages
        - mountPath: /root/.kube/
          name: kube-config
        - mountPath: /cpp_work
          name: cpp
        - mountPath: /share/data/external-datasets
          name: externalimagefiles
      restartPolicy: Never
      backoffLimit: 32
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
      - name: externalimagefiles
        persistentVolumeClaim:
          claimName: external-images-pvc

""")

def is_debug():
    """
    Check if the users has the debug env.var. set
    """
    debug = False
    if os.environ.get('DEBUG'):
        debug = True
    
    #logging.info("debug=" + str(debug))

    return debug

def get_namespace():
    if is_debug():
        namespace = 'cpp-debug'
    else:
        namespace = 'cpp'
    return namespace


def init_kubernetes_connection():

    # load the kube config
    kubernetes.config.load_kube_config(str(pathlib.Path.home()) + '/.kube/config')


def load_cpp_config():


    # fetch db settings
    namespace = get_namespace()
    logging.info("namespace:" + namespace)
    
    if is_debug():
        with open('/cpp/configs_debug.yaml', 'r') as configs_debug:
            cpp_config = yaml.load(configs_debug, Loader=yaml.FullLoader)
   
    else:
        configmap = kubernetes.client.CoreV1Api().read_namespaced_config_map("cpp-configs", namespace)
        cpp_config = yaml.load(configmap.data['configs.yaml'], Loader=yaml.FullLoader)

        # fetch db secret
        secret = kubernetes.client.CoreV1Api().read_namespaced_secret("postgres-password", "cpp")
        postgres_password = base64.b64decode(secret.data['password.postgres']).decode().strip()
        cpp_config['postgres']['password'] = postgres_password


    return cpp_config


def connect_db(cpp_config):


    # connect to the db
    logging.warning("Connecting to db.")
    connection = None
    connection = psycopg2.connect(  database=cpp_config['postgres']['db'], 
                                    user=cpp_config['postgres']['user'],
                                    host=cpp_config['postgres']['host'],
                                    port=cpp_config['postgres']['port'], 
                                    password=cpp_config['postgres']['password'])

    # make results into dicts
    cursor = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)

    return connection, cursor


def generate_random_identifier(length):

    return ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) for _ in range(length))


def handle_new_jobs(cursor, connection, job_limit=None):

    # ask for all new analyses
    logging.info('Running analyses query.')
    cursor.execute('''
                        SELECT *
                        FROM image_sub_analyses
                        WHERE start IS NULL
                        ORDER by sub_id 
                       ''')
    analyses = cursor.fetchall()

    # for all unstarted analyses 
    for analysis in analyses:
        # skip analyiss if there are unmet dependencies
        if not all_dependencies_satisfied(analysis, cursor):
            continue

        # Check if kubernetes job queue is empty if not skip
        if not is_kubernetes_job_queue_empty():
            continue

        # check the analysis type and process by analysis specific function
        if analysis['meta']['type'] == 'cellprofiler':
            handle_analysis_cellprofiler(analysis, cursor, connection, job_limit)

        elif analysis['meta']['type'] == 'jupyter_notebook':
            handle_anlysis_jupyter_notebook(analysis, cursor, connection)

        else:
            raise ValueError(f'Unknown Analysis type: {analysis["meta"]["type"]} in subanalysis id {analysis["sub_id"]}')



def handle_anlysis_jupyter_notebook(analysis, cursor, connection):

    analysis_id = analysis["analysis_id"]
    sub_analysis_id = analysis["sub_id"]
    acquisition_id = analysis["plate_acquisition_id"]
    
    logging.info('Inside handle_anlysis_jupyter_notebook')

    notebook_file = "/cpp_work/notebooks/" + analysis["meta"]["notebook_file"]

    logging.info('Notebook file:' + notebook_file)

    # if indata is from a previous analysis (Not implemented)
    if "indata_analysis_id" in analysis["meta"]:
        indata_analysis_id = analysis["meta"][ "indata_analysis_id"]
        input_storage_paths = get_storage_paths_from_analysis_id(cursor, indata_analysis_id)
        analyis_input_folder = input_storage_paths['full']
        analysis_input_file = "Nothing"
    else:
        input_storage_paths = get_storage_paths_from_analysis_id(cursor, analysis_id)
        analyis_input_folder = input_storage_paths['full']
        analysis_input_file = "Nothing"

    # To do - create general method for this (its duplicated in handle_analysis_cellprofiler) 
    # generate names
    random_identifier = generate_random_identifier(8)
    job_number = 0;
    n_jobs = 1
    job_id = create_job_id(analysis_id, sub_analysis_id, random_identifier, job_number, n_jobs)
    output_path = f"/cpp_work/output/cpp-worker-job-{job_id}/notebooks/"
    job_name = f"cpp-worker-job-{job_id}"

    # copy notebook file to output folder (Convertion is done with nbconvert --inplace)
    copyof_notebook_file = output_path + os.path.basename(notebook_file)
    os.makedirs(os.path.dirname(copyof_notebook_file), exist_ok=True)
    shutil.copyfile(notebook_file, copyof_notebook_file)

    job_yaml = make_jupyter_yaml(copyof_notebook_file, output_path, job_name, analysis_id, sub_analysis_id, analyis_input_folder, analysis_input_file)

    logging.info("yaml:" + yaml.dump( job_yaml, default_flow_style=False, default_style='' ))

    k8s_batch_api = kubernetes.client.BatchV1Api()
    resp = k8s_batch_api.create_namespaced_job(
                 body=job_yaml, namespace=getNamespace())
    logging.info(f"Deployment created. status='{resp.metadata.name}'")

     # when all chunks of the sub analysis are sent in, mark the sub analysis as started
    mark_analysis_as_started(cursor, connection, analysis_id)
    mark_sub_analysis_as_started(cursor, connection, sub_analysis_id)

#    # generate the paths needed
#    plate_barcode, acquisition_id, analysis_id = get_plate_info(cursor, analysis['sub_id'])
#    storage_root = {"full": f"/cpp_work/results/{plate_barcode}/{acquisition_id}/{analysis_id}", "mount_point":"/cpp_work/", "job_specific":f"results/{plate_barcode}/{acquisition_id}/{analysis_id}/"}


def handle_analysis_cellprofiler(analysis, cursor, connection, job_limit=None):

        analysis_id = analysis["analysis_id"]
        sub_analysis_id = analysis["sub_id"]

        # fetch the channel map for the acqusition
        logging.info('Running channel map query.')
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
        
        # make sure channel map is populated
        if len(channel_map) == 0:
            raise ValueError('Channel map is empty, possible error in plate acqusition id.')

        # fetch all images belonging to the plate acquisition
        logging.info('Fetching images belonging to plate acqusition.')
        
        query = f"""
                            SELECT *
                            FROM images_all_view
                            WHERE plate_acquisition_id='{analysis['plate_acquisition_id']}'
                            ORDER BY timepoint, well, site, channel
                           """ # also NOT IN (select * from images_analysis where analysed=None) or something
        cursor.execute(query)
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


        # get analysis settings
        try:
            cellprofiler_settings = analysis['meta']
        except KeyError:
            logging.error(f"Unable to get cellprofiler settings for analysis: sub_id={sub_analysis_id}")

        # get cellprofiler-version
        try:
            cellprofiler_version = cellprofiler_settings['cellprofiler_version']
        except KeyError:
            logging.error(f"Unable to get cellprofiler_version details from analysis entry: sub_id={sub_analysis_id}")
            cellprofiler_version = "v4.0.7"

        # check if all imgsets should be in the same job
        try:
            chunk_size = cellprofiler_settings['batch_size'] 
            pipeline_file = '/cpp_work/pipelines/' + cellprofiler_settings['pipeline_file']
        except KeyError:
            logging.error(f"Unable to get cellprofiler details from analysis entry: sub_id={sub_analysis_id}")
            chunk_size = 1
        if chunk_size <= 0:
            # put them all in the same job if chunk size is less or equal to zero
            chunk_size = len(imgsets)


        # calculate the number of chunks that will be created
        n_imgsets = len(imgsets)
        n_jobs_unrounded = n_imgsets / chunk_size
        n_jobs = math.ceil(n_jobs_unrounded)

        # get common output for all sub analysis
        storage_paths = get_storage_paths_from_analysis_id(cursor, analysis_id)
        # Make sure output dir exists
        os.makedirs(f"{storage_paths['full']}", exist_ok=True)

        
        # create chunks and submit as separate jobs
        random_identifier = generate_random_identifier(8)
        for i,imgset_chunk in enumerate(chunk_dict(img_infos, chunk_size)):

            # generate names
            job_number = i
            job_id = create_job_id(analysis_id, sub_analysis_id, random_identifier, job_number, n_jobs)
            imageset_file = f"/cpp_work/input/cpp-worker-job-{job_id}.csv"
            output_path = f"/cpp_work/output/cpp-worker-job-{job_id}/"
            job_name = f"cpp-worker-job-{job_id}"
            job_timeout = cellprofiler_settings.get('job_timeout', "10800")
            job_yaml = make_cellprofiler_yaml(cellprofiler_version, pipeline_file, imageset_file, output_path, job_name, analysis_id, sub_analysis_id, job_timeout)

            # Check if icf headers should be added to imgset csv file, default is False
            use_icf = cellprofiler_settings.get('use_icf', False)
            logging.info("use_icf" + str(use_icf))
             # generate cellprofiler imgset file for this imgset
            imageset_content = make_imgset_csv(imgsets=imgset_chunk, channel_map=channel_map, storage_paths=storage_paths, use_icf=use_icf)
            with open(imageset_file, 'w') as imageset_csv:
                imageset_csv.write(imageset_content)
            
            k8s_batch_api = kubernetes.client.BatchV1Api()
#            print(dep)
            resp = k8s_batch_api.create_namespaced_job(
                    body=job_yaml, namespace=get_namespace())
            logging.info(f"Deployment created. status='{resp.metadata.name}'")

            if job_limit is not None and i >= (job_limit-1):
                print("exit here")
                break

        # when all chunks of the sub analysis are sent in, mark the sub analysis as started
        mark_analysis_as_started(cursor, connection, analysis['analysis_id'])
        mark_sub_analysis_as_started(cursor, connection, analysis['sub_id'])


def is_kubernetes_job_queue_empty():

    logging.info("Inside is_kubernetes_job_queue_empty");

    # list all jobs in namespace
    k8s_batch_api = kubernetes.client.BatchV1Api()
    job_list = k8s_batch_api.list_namespaced_job(namespace=get_namespace())

    # filter out all finished jobs
    # logging.info("job-list len" + str(len(job_list.items)))

    is_queue_empty = True
    for job in job_list.items:
      if job.status.start_time is None:
        is_queue_empty = False
        logging.info("Queue is not empty, exit loop")
        break

    logging.info("Finished is_kubernetes_job_queue_empty, is_queue_empty=:" + str(is_queue_empty))
    return is_queue_empty




def fetch_finished_job_families(cursor, connection, job_limit = None):

    # list all jobs in namespace
    k8s_batch_api = kubernetes.client.BatchV1Api()
    job_list = k8s_batch_api.list_namespaced_job(namespace=get_namespace())
    
    # filter out all finished jobs
    finished_jobs = {}
    for job in job_list.items:

        # convert to dict for usability
        job_dict = job.to_dict()

        # if the job has not started yet
        if job_dict['status']['conditions'] == None:
            continue

        # if the job's state is compelted, save it in a new dict with job name as key
        if job_dict['status']['conditions'][0]['type'] == 'Complete':
            finished_jobs[job_dict['metadata']['name']] = job_dict 

        elif job_dict['status']['conditions'][0]['type'] == 'Failed':
            mark_sub_analysis_as_failed(cursor, connection, job_dict)


    logging.info("Finished jobs done " + str(len(finished_jobs)))

    # continue processing the finished jobs
    job_buckets = {}
    for job_name,job in finished_jobs.items():

        # get the family name
        job_family = get_job_family_from_job_name(job_name)

        # append all jobs with the same family name into a list
        try:
            job_buckets[job_family].append(job)

        except KeyError:
            job_buckets[job_family] = [job]


    logging.info("Finished buckets: " + str(len(job_buckets)))

    # fetch each familys total number of jobs and compare with the total count
    family_job_count = {}
    finished_families = {}
    for family_name, job_list in job_buckets.items():
        
        # save the total job count for this family
        family_job_count = get_family_job_count_from_job_name(job_list[0]['metadata']['name'])
        logging.info(f"fjc: {family_job_count}\tjl: {len(job_list)}")
        # check if there are as many finished jobs as the total job count for the family
        # for debug reasons we also check if the job limit is reached
        if family_job_count == len(job_list) or (job_limit is not None and len(job_list) == job_limit):

            # then all jobs in this family are finished and ready to be processed
            finished_families[family_name] = job_list


    logging.info("Finished families: " + str(len(finished_families)))
    return finished_families

    



# goes through all jobs of a family i.e. merges the csvs with the same names into
# a single resulting csv file for the entire family, e.g. ..._Experiment.csv, ..._Image.csv
def merge_family_jobs_csv_old(family_name, job_list):

    # init
    merged_csvs = {}

    # for each job in the family
    for job in job_list:
        
        # fetch all csv files in the job folder
        job_path = f"/cpp_work/output/{job['metadata']['name']}"
        for csv_file in pathlib.Path(job_path).rglob("*.csv"):

            filename = str(csv_file).replace(job_path+'/', '')

            # init the file entry if needed
            if filename not in merged_csvs:
                merged_csvs[filename] = {}
                merged_csvs[filename]['rows'] = []

            # read the csv
            with open(csv_file, 'r') as csv_file_handle:
                reader = csv.reader(csv_file_handle)

                # svae the first row as header
                merged_csvs[filename]['header'] = reader.__next__()

                # append the remaining rows as content
                for row in reader:
                    merged_csvs[filename]['rows'].append(row)


    return merged_csvs

    

# goes through all jobs of a family i.e. merges the csvs with the same names into
# a single resulting csv file for the entire family, e.g. ..._Experiment.csv, ..._Image.csv
def merge_family_jobs_csv(family_name, job_list):

    logging.debug("Inside merge_family_jobs_csv")

    logging.debug("job_list:" + str(job_list))

    # init
    merged_csvs = {}

    # for each job in the family
    for job in job_list:
        
        # fetch all csv files in the job folder
        job_path = f"/cpp_work/output/{job['metadata']['name']}"
        for csv_file in pathlib.Path(job_path).rglob("*.csv"):

            # keep only the path relative to the job_path
            filename = str(csv_file).replace(job_path+'/', '')

            logging.debug("filename" + str(filename))

            # init the file entry if needed
            if filename not in merged_csvs:
                merged_csvs[filename] = {}
                merged_csvs[filename]['rows'] = []

            # read the csv
            with open(csv_file, 'r') as csv_file_handle:

                # svae the first row as header
                merged_csvs[filename]['header'] = csv_file_handle.readline()

                # append the remaining rows as content
                for row in csv_file_handle:
                    merged_csvs[filename]['rows'].append(row)

    logging.debug("done merge_family_jobs_csv")

    return merged_csvs



# goes through all the non-csv filescsv of a family of job and copies the result to the result folder
def copy_job_results_to_storage(family_name, job_list, storage_root, files_created):

    logging.debug("inside copy_job_results_to_storage")

    # for each job in the family
    for job in job_list:
        
        # fetch all files in the job folder
        job_path = f"/cpp_work/output/{job['metadata']['name']}"
        for result_file in pathlib.Path(job_path).rglob("*"):

            logging.debug("copy file: " + str(result_file))

            # exclude files with these extensions
            if result_file.suffix in ['.csv'] or pathlib.Path.is_dir(result_file):
                logging.debug("continue")
                continue

            # keep only the path relative to the job_path
            filename = str(result_file).replace(job_path+'/', '')

            # create a folder for the file if needed
            subdir_name = os.path.dirname(filename)
            os.makedirs(f"{storage_root['full']}/{subdir_name}", exist_ok=True)

            # copy the file to the storage location
            shutil.copyfile(f"{job_path}/{filename}", f"{storage_root['full']}/{filename}")

            # remember the file
            files_created.append(f"{filename}")

            logging.debug("done copy file: " + str(filename))


    logging.debug("done copy_job_results_to_storage")

    return files_created



def create_job_id(analysis_id, sub_analysis_id, random_identifier, job_number, n_jobs):
    return f"{sub_analysis_id}-{random_identifier}-{job_number}-{n_jobs}-{analysis_id}"

def get_family_job_count_from_job_name(job_name):
    match = re.match('cpp-worker-job-\d+-\w+-\d+-(\d+)', job_name)
    return int(match.groups()[0])

def get_job_family_from_job_name(job_name):
    match = re.match('(cpp-worker-job-\d+-\w+)', job_name)
    return match.groups()[0]

def get_analysis_sub_id_from_path(path):
    match = re.match('cpp-worker-job-(\w+)-', path)
    return int(match.groups()[0])

def get_analysis_sub_id_from_family_name(family_name):
    match = re.match('cpp-worker-job-(\w+)-', family_name)
    return int(match.groups()[0])
    




def get_analysis_info(cursor, analysis_id):

    # fetch all images belonging to the plate acquisition
    logging.info('Fetching plate info from view.')
    query = f"""
                        SELECT *
                        FROM image_analyses_v1
                        WHERE id='{analysis_id}'
                       """ # also NOT IN (select * from images_analysis where analysed=None) or something

    logging.info(query)
    cursor.execute(query)
    plate_info = cursor.fetchone()

    return plate_info

def get_sub_analysis_info(cursor, analysis_sub_id):

    # fetch all images belonging to the plate acquisition
    logging.info('Fetching plate info from view1')
    query = f"""
                        SELECT *
                        FROM image_sub_analyses_v1
                        WHERE sub_id=%s
                       """ # also NOT IN (select * from images_analysis where analysed=None) or something

    logging.info(query)

    cpp_config = load_cpp_config()
    connection, cursor2 = connect_db(cpp_config)

    cursor2.execute(query, (analysis_sub_id,))
    plate_info = cursor2.fetchone()
    cursor2.close()
    connection.close()

    logging.info("plate_info:" + str(plate_info))

    if plate_info is None:
        logging.error("plate_info is None, sub_id not found, should not be able to happen....")

    return plate_info






def write_csv_to_storage(merged_csvs, storage_root):

    logging.debug("Inside write_csv_to_storage")

    # remember created files
    files_created = []

    # loop over all csv files in the dict
    for csv_filename in merged_csvs:

        logging.debug("csv_filename" + csv_filename)

        # make dir if needed
        subdir_name = os.path.dirname(csv_filename)
        os.makedirs(f"{storage_root['full']}/{subdir_name}", exist_ok=True)

        with open(f"{storage_root['full']}/{csv_filename}", 'w', newline='') as csv_file_handle:
            #pdb.set_trace()
            logging.debug("inside_write" )
            csv_file_handle.write((merged_csvs[csv_filename]['header']))
            for line in merged_csvs[csv_filename]['rows']:
                csv_file_handle.write(line)
            
            logging.debug("write done" )

        files_created.append(f"{csv_filename}")

    logging.debug("Done write_csv_to_storage")

    return files_created





def insert_sub_analysis_results_to_db(connection, cursor, sub_analysis_id, storage_root,  file_list):

    # fetch the result json
    query = f"""SELECT result
                FROM image_sub_analyses
                WHERE sub_id={sub_analysis_id};
            """
    cursor.execute(query)
    row = cursor.fetchone()

    # update the file list
    result = row['result']
    if not result:
        result = {}

    logging.debug("result:" + str(result))

    ### include the job specific folder name into file path

    # martin way
    # result['file_list'] = [storage_root['job_specific']+file_name for file_name in file_list]

    # anders way
    file_list_with_job_specific_path = []
    for file_name in file_list:
        file_name_with_job_specific_path = storage_root['job_specific'] + file_name
        file_list_with_job_specific_path.append(file_name_with_job_specific_path)
    result['job_folder'] = storage_root['job_specific']
    result['file_list'] = file_list_with_job_specific_path


    # maybe in the future we should do a select first and 
    query = f"""UPDATE image_sub_analyses
                SET result=%s,
                    finish=%s
                WHERE sub_id=%s
            """
    logging.debug("query:" + str(query))
    cursor.execute(query, [json.dumps(result), datetime.datetime.now(), sub_analysis_id])
    logging.debug("Before commit")
    connection.commit()
    logging.debug("Commited")

    delete_job(sub_analysis_id)
    


# go through unfinished analyses and wrap them up if possible
def handle_finished_analyses(cursor, connection):

    # fetch all unfinished analyses
    cursor.execute(f"""
        SELECT *
        FROM image_analyses
        WHERE finish IS NULL AND error IS NULL
        """) # also NOT IN (select * from images_analysis where analysed=None) or something
    analyses = cursor.fetchall()


    # go through the unfinished analyses
    for analysis in analyses:

        ### check if all sub analyses for the analysis are finised

        # get all sub analysis belonging to the analysis

        # fetch all unfinished analyses
        
            
        sql = (f"""
            SELECT *
            FROM image_sub_analyses
            WHERE analysis_id={analysis['id']}
            """) # also NOT IN (select * from images_analysis where analysed=None) or something

        logging.debug("sql" + sql)
        
        cursor.execute(sql)

        sub_analyses = cursor.fetchall()


        # init booleans
        only_finished_subs = True
        no_failed_subs = True
        file_list = []

        # for each sub analysis
        for sub_analysis in sub_analyses:

            # check if it is unfinished
            if not sub_analysis['finish']:
            
                # if so, flag to move on to the next analysis
                only_finished_subs = False

            # check if it ended unsuccessfully
            if sub_analysis['error']:

                # if so, mark the analysis as failed and contact an adult
                no_failed_subs = False

            # else save the file list
            if only_finished_subs and no_failed_subs:
                file_list += sub_analysis['result']['file_list']
                job_folder = sub_analysis['result']['job_folder']


        # if all sub analyses were successfully finished, save the total file list and finish time in the db
        if only_finished_subs and no_failed_subs:

            # create the result dict
            result = {'file_list': file_list, 'job_folder': job_folder}

            # create timestamp
            finish = datetime.datetime.now()

            # construct query
            query = f""" UPDATE image_analyses
                        SET finish=%s,
                            result=%s
                        WHERE id=%s
            """
            cursor.execute(query, [finish, json.dumps(result), analysis['id'], ])
            connection.commit()


        # if any sub analysis failed, mark the analysis as failed as well
        elif not no_failed_subs:

            # create timestamp
            error = str(datetime.datetime.now())

            # construct query
            query = f""" UPDATE image_analyses
                        SET error=%s
                        WHERE id=%s
            """
            cursor.execute(query, [error, analysis['id']])
            connection.commit()











def delete_job(sub_analysis_id):

    namespace = get_namespace()
    logging.info('Inside delete_job')
    

    # list all jobs in namespace
    k8s_batch_api = kubernetes.client.BatchV1Api()
    job_list = k8s_batch_api.list_namespaced_job(namespace=namespace)
    
    # filter out all finished jobs
    for job in job_list.items:

        # convert to dict for usability
        job_dict = job.to_dict()
        job_name = job_dict['metadata']['name']


        # check if the job belongs to the sub analysis to delete
        if job_name.startswith(f"cpp-worker-job-{sub_analysis_id}-"):

            # detele the job
#            pdb.set_trace()
            logging.debug("Delete job:" + job_name)
            response = k8s_batch_api.delete_namespaced_job(job_name, namespace, propagation_policy='Foreground') # background is also possible, no idea about difference
            logging.warning(f"Deleting job {job_name}")
            logging.info(f"Deleting job: {str(response)}")







def mark_sub_analysis_as_failed(cursor, connection, job):

    # get job name
    job_name = job['metadata']['name']

    # get sub analysis id
    sub_analysis_id = get_analysis_sub_id_from_family_name(job_name)

    query = """ UPDATE image_sub_analyses
                SET error=%s
                WHERE sub_id=%s
    """
    cursor.execute(query, [str(datetime.datetime.now()), sub_analysis_id,])
    connection.commit()


    #delete_job(sub_analysis_id)




def mark_analysis_as_started(cursor, connection, analysis_id):

    # only update start times where none is set
    query = """ UPDATE image_analyses
                SET start=%s
                WHERE id=%s
                AND start IS NULL
    """
    cursor.execute(query, [str(datetime.datetime.now()), analysis_id,])
    connection.commit()




def mark_sub_analysis_as_started(cursor, connection, sub_analysis_id):

    query = """ UPDATE image_sub_analyses
                SET start=%s
                WHERE sub_id=%s
    """
    cursor.execute(query, [str(datetime.datetime.now()), sub_analysis_id,])
    connection.commit()








def reset_debug_jobs(analysis_id, sub_analysis_id, connection=None, cursor=None):

    
    query = f"""

UPDATE image_sub_analyses
SET start=NULL,
    error=NULL,
    finish=NULL,
    result=NULL
WHERE sub_id={sub_analysis_id};

    """
    cursor.execute(query)

#    pdb.set_trace()

    query = f"""


UPDATE image_analyses
SET start=NULL,
    error=NULL,
    finish=NULL,
    result=NULL
WHERE id={analysis_id};

    """
    cursor.execute(query)
    connection.commit()


def get_storage_paths_from_analysis_id(cursor, analysis_id):

    analysis_info = get_analysis_info(cursor, analysis_id)
    
    plate_barcode = analysis_info["plate_barcode"]
    acquisition_id = analysis_info["plate_acquisition_id"]

    storage_paths = {
        "full": f"/cpp_work/results/{plate_barcode}/{acquisition_id}/{analysis_id}",
        "mount_point":"/cpp_work/",
        "job_specific":f"results/{plate_barcode}/{acquisition_id}/{analysis_id}/"
        }
    return storage_paths


def get_storage_paths_from_sub_analysis_id(cursor, sub_analysis_id):

    logging.info("Inside get_storage_paths_from_sub_analysis_id")

    analysis_info = get_sub_analysis_info(cursor, sub_analysis_id)

    plate_barcode = analysis_info["plate_barcode"]
    acquisition_id = analysis_info["plate_acquisition_id"]
    analysis_id =  analysis_info["analyses_id"]

    storage_paths = {
        "full": f"/cpp_work/results/{plate_barcode}/{acquisition_id}/{analysis_id}",
        "mount_point":"/cpp_work/",
        "job_specific":f"results/{plate_barcode}/{acquisition_id}/{analysis_id}/"
        }
    return storage_paths

def main():

    try:

        # set up logging to file
        now = datetime.datetime.now()
        now_string = now.strftime("%Y-%m-%d_%H.%M.%S.%f")
        is_debug_version = ""
        if is_debug():
            is_debug_version = "debug."
        logfile_name = "/cpp_work/logs/cpp_master." + is_debug_version + now_string + ".log"
        logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                            datefmt='%Y-%m-%d:%H:%M:%S',
                            level=logging.INFO,
                            filename=logfile_name,
                            filemode='w')

        # define a Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        # set a formater for console
        consol_fmt = logging.Formatter('%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
        # tell the handler to use this format
        console.setFormatter(consol_fmt)

        # add the handler to the root logger
        logging.getLogger('').addHandler(console)

        first_reset = True
        while True:

            # reset to avoid stale connections
            connection = None
            cursor = None
        
            try:
        
                if is_debug():
                    job_limit = None
                else:
                    job_limit = None

                # init connections
                init_kubernetes_connection()
                cpp_config = load_cpp_config()
                connection, cursor = connect_db(cpp_config)


                # debug function to reset specified jobs to just-submitted state
                if len(sys.argv) > 1 and sys.argv[1] == "reset" and first_reset:
                    reset_debug_jobs(analysis_id=4, sub_analysis_id=4, connection=connection, cursor=cursor)
                    first_reset = False
                    logging.info("Resetting debug jobs.")

                
                handle_new_jobs(cursor, connection, job_limit = job_limit)
            
                finished_families = fetch_finished_job_families(cursor, connection, job_limit = job_limit)
                
                # merge finised jobs for each family (i.e. merge jobs for a sub analysis)
                for family_name, job_list in finished_families.items():
        
                    sub_analysis_id = get_analysis_sub_id_from_family_name(family_name)

                    # final results should be stored in an analysis id based folder e.g. all sub ids beling to the same analyiss id sould be stored in the same folder
                    storage_paths = get_storage_paths_from_sub_analysis_id(cursor, sub_analysis_id)

                    # merge all job csvs into family csv
                    merged_csvs = merge_family_jobs_csv(family_name, job_list)
        #            pdb.set_trace()
                    # write csv to storage location
                    files_created = write_csv_to_storage(merged_csvs, storage_paths)
                    logging.debug("files_created:" + str(files_created))
        
                    # move all other file types to storage
                    files_created = copy_job_results_to_storage(family_name, job_list, storage_paths, files_created)
                    logging.debug("files_created:" + str(files_created))
        
                    # insert csv to db
                    insert_sub_analysis_results_to_db(connection, cursor, sub_analysis_id, storage_paths, files_created)
        
        
                # check for finished analyses
                handle_finished_analyses(cursor, connection)
            
            
            # catch db errors
            except (psycopg2.Error) as error:
                logging.exception(error)
            
            finally:
                #closing database connection
                if connection:
                    cursor.close()
                    connection.close()

            time.sleep(10)

    # Catch all errors
    except Exception as e:
        logging.error(traceback.format_exc())
        # Logs the error appropriately. 


if __name__ == "__main__":


    main()






