#!/usr/bin/env python3



########### HEJ FRAMTIDEN ################
# todo:
# * get channel_map into the database
# * get plate acqusitions into the database
# * fetch db login info from secret
# * fetch only images that have not been analysed from a plate acqusition?
# * store the imgset file as a configmap for each job?
# * fix the job spec yaml, the command and mount paths (root vs user etc)
# * make se worker container image exists and works
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
import json
import string
import itertools
import math
import pathlib
import shutil
import datetime
import time
import pandas as pd
import pyarrow
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import hpc_utils
from database import Database

# divide a dict into smaller dicts with a set number of items in each
def chunk_dict(data, chunk_size=1):

    # create iterator of the dictimport pdb
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

        logging.debug("Inside check_analyses_finished")

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

    header += "Group_Index,Group_Number,ImageNumber,Metadata_Barcode,Metadata_Site,Metadata_Well,Metadata_AcqID,"

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
            row += f'\"{img_filename}\",'

        # add imgset info
        row += f"{imgset_counter},1,{imgset_counter},\"{img['plate_barcode']}\",{img['site']},\"{img['well']}\",{img['plate_acquisition_id']},"

        # add file paths
        for img in sorted_imgset:
            img_dirname = os.path.dirname(img['path'])
            row += f'\"{img_dirname}\",'

        # add file urls
        for img in sorted_imgset:
            path = img['path']
            row += f'\"file:{path}\",'


        # add illumination file names, both as URL_ and PATH_ - these are not uniqe per image,
        # all images with same channel have the same correction image
        if use_icf:
            # First as URL
            for ch_nr,ch_name in sorted(channel_map.items()):
                path = f"{storage_paths['full']}/ICF_{ch_name}.npy"
                row +=  f'\"file:{path}\",'

            # Also as PathName_
            for ch_nr,ch_name in sorted(channel_map.items()):
                dir = f"{storage_paths['full']}"
                row +=  f'"{dir}",'

            # Also as FileName_
            for ch_nr,ch_name in sorted(channel_map.items()):
                row +=  f'\"ICF_{ch_name}.npy\",'

        # remove last comma and add a newline before adding it to the content
        content += row[:-1] + "\n"

    # return the header and the content
    return f"{header}{content}"


def get_cellprofiler_cmd_uppmax(cellprofiler_version, pipeline_file, imageset_file, output_path, job_name, analysis_id, sub_analysis_id, job_timeout, high_prioryty):

    if cellprofiler_version is None:
        cellprofiler_version = "v4.0.7"

    if high_prioryty:
        priority_class_name = "high-priority-cpp"
    else:
        priority_class_name = "low-priority-cpp"

    if is_debug():
       docker_image="ghcr.io/pharmbio/cpp_worker:" + cellprofiler_version + "-latest"
    else:
       docker_image="ghcr.io/pharmbio/cpp_worker:" + cellprofiler_version + "-stable"

    cmd = (f' timeout {job_timeout}'
           f' cellprofiler'
           f' -r'
           f' -c'
           f' -p {pipeline_file}'
           f' --data-file {imageset_file}'
           f' -o {output_path}'
           f' --plugins-directory /CellProfiler/plugins')


    params = (f'{docker_image}'
              f' {pipeline_file}'
              f' {imageset_file}'
              f' {output_path}'
              f' {job_name}'
              f' {analysis_id}'
              f' {sub_analysis_id}'
              f' {job_timeout}'
              f' {high_prioryty}')

    return cmd


def make_cellprofiler_yaml(cellprofiler_version, pipeline_file, imageset_file, output_path, job_name, analysis_id, sub_analysis_id, job_timeout, high_prioryty):

    if cellprofiler_version is None:
        cellprofiler_version = "v4.2.5-cellpose2.0"

    if high_prioryty:
        priority_class_name = "high-priority-cpp"
    else:
        priority_class_name = "low-priority-cpp"

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
  annotations:
    pipeline_file: "{pipeline_file}"
    imageset_file: "{imageset_file}"
    output_path: "{output_path}"
    job_timeout: "{job_timeout}"
    docker_image: "{docker_image}"

spec:
  backoffLimit: 1
  template:
    spec:
      nodeSelector:
        pipelineNode: "true"
      priorityClassName: {priority_class_name}
      securityContext:
        runAsUser: 20000
        fsGroup: 20000  # Ensure the group ID is set so the user can read the SSH key if needed
      containers:
      - name: cpp-worker
        image: {docker_image}
        imagePullPolicy: Always
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
        - name: OMP_NUM_THREADS # This is to prevent multithreading of cellprofiler
          value: "1"
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        #
        # I specify default resources in namespace file now
        #
        volumeMounts:
        - mountPath: /share/mikro/
          name: mikroimages
        - mountPath: /share/mikro2/
          name: mikroimages2
        - mountPath: /share/mikro3/
          name: mikroimages3
        - mountPath: /cpp_work
          name: cpp2
        - mountPath: /share/data/external-datasets
          name: externalimagefiles
      restartPolicy: Never
      volumes:
      - name: mikroimages
        persistentVolumeClaim:
          claimName: micro-images-pvc
      - name: mikroimages2
        persistentVolumeClaim:
          claimName: micro2-images-pvc
      - name: mikroimages3
        persistentVolumeClaim:
          claimName: micro3-images-pvc
      #- name: cpp
      #  persistentVolumeClaim:
      #    claimName: cpp-pvc
      - name: cpp2
        persistentVolumeClaim:
          claimName: cpp2-pvc
      #- name: kube-config
      #  secret:
      #    secretName: cpp-user-kube-config
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
        namespace = 'cpp'
    else:
        namespace = 'cpp'
    return namespace


def init_kubernetes_connection():
    """
    If DEBUG=True, load the local kubeconfig from ~/.kube/config.
    Otherwise (in-cluster) load the service-account credentials.
    """
    if is_debug():
        kube_cfg = pathlib.Path.home() / '.kube' / 'config'
        logging.debug(f"🔍 DEBUG mode: loading local kubeconfig from {kube_cfg}")
        kubernetes.config.load_kube_config(str(kube_cfg))
    else:
        logging.debug("🚀 Production mode: loading in-cluster service account")
        kubernetes.config.load_incluster_config()

    # Grab the loaded config and set it as the default for new ApiClients
    loaded = kubernetes.client.Configuration.get_default_copy()
    kubernetes.client.Configuration.set_default(loaded)

    logging.info(f"✅ Kubernetes client configured to talk to: {loaded.host}")


def load_cpp_config():

    # fetch db settings
    namespace = get_namespace()
    logging.debug("namespace:" + namespace)

    logging.debug(f"is_debug: {is_debug()}")

    if is_debug():
        with open('master/debug_configs.yaml', 'r') as configs_debug:
            cpp_config = yaml.load(configs_debug, Loader=yaml.FullLoader)
            postgres_password = os.environ.get('DB_PASS')
            cpp_config['postgres']['password'] = postgres_password

    else:
        configmap = kubernetes.client.CoreV1Api().read_namespaced_config_map("cpp-configs", namespace)
        cpp_config = yaml.load(configmap.data['configs.yaml'], Loader=yaml.FullLoader)
        secret = kubernetes.client.CoreV1Api().read_namespaced_secret("postgres-password", "cpp")
        # fetch db secret
        postgres_password = base64.b64decode(secret.data['password.postgres']).decode().strip()
        cpp_config['postgres']['password'] = postgres_password

    # fetch uppmax secrets
    cpp_config['uppmax_user'] = cpp_config['uppmax']['user']
    cpp_config['uppmax_hostname'] = cpp_config['uppmax']['hostname']
    cpp_config['uppmax_project'] = cpp_config['uppmax']['project']

    cpp_config['uppmax_user'] = cpp_config['uppmax']['user']
    cpp_config['uppmax_hostname'] = cpp_config['uppmax']['hostname']
    cpp_config['uppmax_project_rackham'] = cpp_config['uppmax']['project_rackham']

    cpp_config['uppmax_user'] = cpp_config['uppmax']['user']
    cpp_config['uppmax_hostname'] = cpp_config['uppmax']['hostname']
    cpp_config['uppmax_project_snowy'] = cpp_config['uppmax']['project_snowy']

    return cpp_config


def connect_db(cpp_config):

    # init new database
    # Initialize new Database connection pool
    db_settings = {
        "host": cpp_config['postgres']['host'],
        "port": cpp_config['postgres']['port'],
        "database": cpp_config['postgres']['db'],
        "user": cpp_config['postgres']['user'],
        "password": cpp_config['postgres']['password']
    }
    Database.get_instance().initialize_connection_pool(**db_settings)

    # connect to the db
    logging.debug("Connecting to db.")

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
    logging.info('Inside handle_new_jobs')
    # ask for all new analyses
    logging.debug('Running analyses query.')
    query = '''
             SELECT *
             FROM image_sub_analyses
             WHERE start IS NULL
             AND error IS NULL
             ORDER by priority, sub_id
            '''
    logging.debug(query)
    cursor.execute(query)

    analyses = cursor.fetchall()

    # first run through all and check for unstarted on uppmax
    for analysis in analyses:
        if analysis['meta']['type'] == 'cellprofiler':
            if 'run_on_uppmax' in analysis['meta'] and analysis['meta']['run_on_uppmax'] == True:
                logging.debug(f'is uppmax analysis: {analysis["analysis_id"]}')
                # only run analyses that have satisfied dependencies
                if all_dependencies_satisfied(analysis, cursor):
                    handle_analysis_cellprofiler_uppmax(analysis, cursor, connection, job_limit)


    # now check for unstarted that should run on cluster
    queue_empty = is_kubernetes_job_queue_empty()
    for analysis in analyses:

        # check the analysis type and process by analysis specific function
        if 'run_on_uppmax' not in analysis['meta'] and 'run_on_dardel' not in analysis['meta'] and 'run_on_hpcdev' not in analysis['meta']:

            logging.debug(f'checking analysis id { analysis["analysis_id"] }')

            priority = analysis['meta'].get('priority', 0)

            # Check if kubernetes job queue is empty or priority is highest
            if not queue_empty and priority != 1:
                break

            # skip analyiss if there are unmet dependencies
            if not all_dependencies_satisfied(analysis, cursor):
                continue

            # check the analysis type and process by analysis specific function
            if analysis['meta']['type'] == 'cellprofiler':
                handle_analysis_cellprofiler(analysis, cursor, connection, job_limit)
                # mark queue as non-empty from now on
                queue_empty = False
            elif analysis['meta']['type'] == 'jupyter_notebook':
                handle_anlysis_jupyter_notebook(analysis, cursor, connection)
            else:
                error_msg = f'Unknown Analysis type: {analysis["meta"]["type"]} in subanalysis id {analysis["sub_id"]}'
                set_sub_analysis_error(cursor,connection, analysis["analysis_id"], analysis["sub_id"], error_msg)
                raise ValueError(error_msg)

        logging.info('done handle_new_jobs')


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
    output_path = f"/cpp_work/output/{sub_analysis_id}/cpp-worker-job-{job_id}/notebooks/"
    job_name = f"cpp-worker-job-{job_id}"

    # copy notebook file to output folder (Convertion is done with nbconvert --inplace)
    copyof_notebook_file = output_path + os.path.basename(notebook_file)
    os.makedirs(os.path.dirname(copyof_notebook_file), exist_ok=True)
    shutil.copyfile(notebook_file, copyof_notebook_file)

    job_yaml = ""

    logging.info("yaml:" + yaml.dump( job_yaml, default_flow_style=False, default_style='' ))

    k8s_batch_api = kubernetes.client.BatchV1Api()
    resp = k8s_batch_api.create_namespaced_job(
                 body=job_yaml, namespace=get_namespace ())
    logging.info(f"Deployment created. status='{resp.metadata.name}'")

     # when all chunks of the sub analysis are sent in, mark the sub analysis as started
    mark_analysis_as_started(cursor, connection, analysis_id)
    mark_sub_analysis_as_started(cursor, connection, sub_analysis_id)

#    # generate the paths needed
#    plate_barcode, acquisition_id, analysis_id = get_plate_info(cursor, analysis['sub_id'])
#    storage_root = {"full": f"/cpp_work/results/{plate_barcode}/{acquisition_id}/{analysis_id}", "mount_point":"/cpp_work/", "job_specific":f"results/{plate_barcode}/{acquisition_id}/{analysis_id}/"}

def parse_string_of_num_and_ranges(input: str):
    """
    Take a string like '2-5,7,15-17,12' and turn it into a list [2, 3, 4, 5, 7, 12, 15, 16, 17]
    """

    if input.startswith("-"):
        return [input]

    numbers = set()

    for element in input.split(','):
        parts = [int(x) for x in element.split('-')]
        if len(parts) == 1:
            numbers.add(parts[0])
        else:
            for part in range(min(parts), max(parts) + 1):
                numbers.add(part)

    return list(numbers)



def handle_analysis_cellprofiler(analysis, cursor, connection, job_limit=None):

        logging.info("analysis: " + str(analysis))

        analysis_id = analysis["analysis_id"]
        sub_analysis_id = analysis["sub_id"]

        # get analysis settings
        try:
            analysis_meta = analysis['meta']
        except KeyError:
            logging.error(f"Unable to get analysis_meta settings for analysis: sub_id={sub_analysis_id}")

        # check if sites filter is included
        site_filter = None
        if 'site_filter' in analysis_meta:
            site_filter = list(analysis_meta['site_filter'])

        # check if well filter is included
        well_filter = None
        if 'well_filter' in analysis_meta:
            well_filter = list(analysis_meta['well_filter'])

        channels_filter = None
        if 'channels' in analysis_meta:
            channels_filter = list(analysis_meta['channels'])

        # check if z filter is included
        # otherwise set default (median)
        z_filter = None
        if 'z' in analysis_meta:
            z_filter = parse_string_of_num_and_ranges(analysis_meta['z'])
        else:
            # Retrieve the median z plane, then make a list out of it
            z_value = get_middle_z_plane(cursor, analysis['plate_acquisition_id'])
            z_filter = [z_value]


        # fetch all images belonging to the plate acquisition
        logging.info('Fetching images belonging to plate acqusition.')

        query = ("SELECT DISTINCT plate_acquisition_id, plate_barcode, timepoint, well, site, z, channel, dye, path"
                 " FROM images_all_view"
                 " WHERE plate_acquisition_id=%s"
        )

        if site_filter:
            query += f' AND site IN ({ ",".join( map( str, site_filter )) }) '

        if well_filter:
            query += ' AND well IN (' + ','.join("'{0}'".format(well) for well in well_filter) + ")"

        if channels_filter:
            query += ' AND dye IN (' + ','.join("'{0}'".format(chan) for chan in channels_filter) + ")"

        if z_filter:
            query += ' AND z IN (' + ','.join("'{0}'".format(z) for z in z_filter) + ")"

        query += " ORDER BY timepoint, well, site, channel"

        logging.info("query: " + query)

        cursor.execute(query, [ analysis['plate_acquisition_id']])
        imgs = cursor.fetchall()

        # if imgs is empty raise error
        if not imgs:
            errormessage = "No images found for the analysis with the input data ...."
            set_sub_analysis_error(cursor, connection, analysis_id, sub_analysis_id, errormessage)
            raise ValueError(errormessage)

        # make imgsets of result
        imgsets = {}
        img_infos = {}
        for img in imgs:

            # readability
            imgset_id = f"tp{img['timepoint']}-{img['well']}-{img['site']}-{img['z']}"

            # if it has been seen before
            try:
                imgsets[imgset_id] += [img['path']]
                img_infos[imgset_id] += [img]
            # if it has not been seen before
            except KeyError:
                imgsets[imgset_id] = [img['path']]
                img_infos[imgset_id] = [img]

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
        # make sure channel map is populated

        channel_map = {}
        # Check if the analysis_meta channels list is provided; if not, include all results without filtering
        if channels_filter is None:
            # Include all channels from the results without filtering
            for channel in channel_map_res:
                channel_map[channel['channel']] = channel['dye']
        else:
            # Filter the results to include only those where the dye is in the 'channels' list
            for channel in channel_map_res:
                if channel['dye'] in channels_filter:
                    channel_map[channel['channel']] = channel['dye']

        if len(channel_map) == 0:
            set_sub_analysis_error(cursor, connection, analysis_id, sub_analysis_id, 'Channel map is empty')
            raise ValueError('Channel map is empty, possible error in plate acqusition id.')

        # get cellprofiler-version
        try:
            cellprofiler_version = analysis_meta['cp_version']
        except KeyError:
            logging.error(f"Unable to get cellprofiler_version details from analysis entry: sub_id={sub_analysis_id}")
            cellprofiler_version = None

        # check if all imgsets should be in the same job
        try:
            chunk_size = analysis_meta['batch_size']
            pipeline_file = '/cpp_work/pipelines/' + analysis_meta['pipeline_file']
        except KeyError:
            logging.error(f"Unable to get cellprofiler details from analysis entry: sub_id={sub_analysis_id}")
            chunk_size = 1
        if chunk_size <= 0:
            # put them all in the same job if chunk size is less or equal to zero
            chunk_size = max(1, len(imgsets))

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
            imageset_file = f"/cpp_work/input/{sub_analysis_id}/cpp-worker-job-{job_id}.csv"
            job_yaml_file = f"/cpp_work/input/{sub_analysis_id}/cpp-worker-job-{job_id}.yaml"
            output_path = f"/cpp_work/output/{sub_analysis_id}/cpp-worker-job-{job_id}/"
            job_name = f"cpp-worker-job-{job_id}"

            logging.debug(f"job_timeout={analysis_meta.get('job_timeout')}")

            job_timeout = analysis_meta.get('job_timeout', "10800")
            priority = analysis_meta.get('priority', 0)
            if priority == 1:
                high_priority = True
            else:
                high_priority = False
            job_yaml = make_cellprofiler_yaml(cellprofiler_version, pipeline_file, imageset_file, output_path, job_name, analysis_id, sub_analysis_id, job_timeout, high_priority)

            # Check if icf headers should be added to imgset csv file, default is False
            use_icf = analysis_meta.get('use_icf', False)
            logging.debug("use_icf" + str(use_icf))
             # generate cellprofiler imgset file for this imgset
            imageset_content = make_imgset_csv(imgsets=imgset_chunk, channel_map=channel_map, storage_paths=storage_paths, use_icf=use_icf)

            # create a folder for the file if needed
            os.makedirs(os.path.dirname(imageset_file), exist_ok=True)
            # write csv
            with open(imageset_file, 'w') as file:
                file.write(imageset_content)

            # save yaml for debugging purposes
            with open(job_yaml_file, 'w') as file:
                yaml.dump(job_yaml, file, default_flow_style=False)


            k8s_batch_api = kubernetes.client.BatchV1Api()
#            print(dep)
            resp = k8s_batch_api.create_namespaced_job(
                    body=job_yaml, namespace=get_namespace())
            logging.debug(f"Deployment created. status='{resp.metadata.name}'")

            if job_limit is not None and i >= (job_limit-1):
                print("exit here")
                break

        # when all chunks of the sub analysis are sent in, mark the sub analysis as started
        mark_analysis_as_started(cursor, connection, analysis['analysis_id'])
        mark_sub_analysis_as_started(cursor, connection, analysis['sub_id'])

def handle_analysis_cellprofiler_uppmax(analysis, cursor, connection, job_limit=None):

        logging.info("analysis: " + str(analysis))

        analysis_id = analysis["analysis_id"]
        sub_analysis_id = analysis["sub_id"]

        # get analysis settings
        try:
            analysis_meta = analysis['meta']
        except KeyError:
            logging.error(f"Unable to get analysis_meta settings for analysis: sub_id={sub_analysis_id}")

        # check if sites filter is included
        site_filter = None
        if 'site_filter' in analysis_meta:
            site_filter = list(analysis_meta['site_filter'])

        # check if well filter is included
        well_filter = None
        if 'well_filter' in analysis_meta:
            well_filter = list(analysis_meta['well_filter'])

        channels_filter = None
        if 'channels' in analysis_meta:
            channels_filter = list(analysis_meta['channels'])

        # check if z filter is included
        # otherwise set default
        z_filter = None
        if 'z' in analysis_meta:
            z_filter = parse_string_of_num_and_ranges(analysis_meta['z'])
        else:
            # Retrieve the median z plane, then make a list out of it
            z_value = get_middle_z_plane(cursor, analysis['plate_acquisition_id'])
            z_filter = [z_value]

        logging.info(f"z_filter: {z_filter}")

        # fetch all images belonging to the plate acquisition
        logging.info('Fetching images belonging to plate acqusition.')

        query = ("SELECT DISTINCT plate_acquisition_id, plate_barcode, timepoint, well, site, z, channel, dye, path"
                 " FROM images_all_view"
                 " WHERE plate_acquisition_id=%s"
        )

        if site_filter:
            query += f' AND site IN ({ ",".join( map( str, site_filter )) }) '

        if well_filter:
            query += ' AND well IN (' + ','.join("'{0}'".format(well) for well in well_filter) + ")"

        if channels_filter:
            query += ' AND dye IN (' + ','.join("'{0}'".format(chan) for chan in channels_filter) + ")"

        if z_filter:
            query += ' AND z IN (' + ','.join("'{0}'".format(z) for z in z_filter) + ")"

        query += " ORDER BY timepoint, well, site, channel"

        logging.info("query: " + query)

        cursor.execute(query, [ analysis['plate_acquisition_id']])
        imgs = cursor.fetchall()

        # if imgs is empty raise error
        if not imgs:
            errormessage = "No images found for the analysis with the input data ...."
            set_sub_analysis_error(cursor, connection, analysis_id, sub_analysis_id, errormessage)
            raise ValueError(errormessage)

        # make imgsets of result
        imgsets = {}
        img_infos = {}
        for img in imgs:

            # readability
            imgset_id = f"tp{img['timepoint']}-{img['well']}-{img['site']}-{img['z']}"

            # if it has been seen before
            try:
                imgsets[imgset_id] += [img['path']]
                img_infos[imgset_id] += [img]
            # if it has not been seen before
            except KeyError:
                imgsets[imgset_id] = [img['path']]
                img_infos[imgset_id] = [img]

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
        # make sure channel map is populated

        channel_map = {}

        logging.debug(f"channels_filter {channels_filter}")
        logging.debug(f"channel_map_res {channel_map_res}")
        # Check if the analysis_meta channels list is provided; if not, include all results without filtering
        if channels_filter is None:
            # Include all channels from the results without filtering
            for channel in channel_map_res:
                channel_map[channel['channel']] = channel['dye']
        else:
            # Filter the results to include only those where the dye is in the 'channels' list
            for channel in channel_map_res:
                logging.debug(f"channel {channel}")
                if channel['dye'] in channels_filter:
                    logging.debug(f"channel[d'ye'] {channel['dye']}")
                    channel_map[channel['channel']] = channel['dye']

        if len(channel_map) == 0:
            set_sub_analysis_error(cursor, connection, analysis_id, sub_analysis_id, 'Channel map is empty')
            raise ValueError('Channel map is empty, possible error in plate acqusition id.')

        # get cellprofiler-version
        try:
            cellprofiler_version = analysis_meta['cp_version']
        except KeyError:
            logging.error(f"Unable to get cellprofiler_version details from analysis entry: sub_id={sub_analysis_id}")
            cellprofiler_version = None

        # check if all imgsets should be in the same job
        try:
            chunk_size = analysis_meta['batch_size']
            pipeline_file = '/cpp_work/pipelines/' + analysis_meta['pipeline_file']
        except KeyError:
            logging.error(f"Unable to get cellprofiler details from analysis entry: sub_id={sub_analysis_id}")
            chunk_size = 1
        if chunk_size <= 0:
            # put them all in the same job if chunk size is less or equal to zero
            chunk_size = max(1, len(imgsets))


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
        all_cmds = []
        for i,imgset_chunk in enumerate(chunk_dict(img_infos, chunk_size)):

            # generate names
            job_number = i
            job_id = create_job_id(analysis_id, sub_analysis_id, random_identifier, job_number, n_jobs)
            imageset_file = f"/cpp_work/input/{sub_analysis_id}/cpp-worker-job-{job_id}.csv"
            job_yaml_file = f"/cpp_work/input/{sub_analysis_id}/cpp-worker-job-{job_id}.yaml"
            output_path = f"/cpp_work/output/{sub_analysis_id}/cpp-worker-job-{job_id}/"
            job_name = f"cpp-worker-job-{job_id}"

            logging.debug(f"job_timeout={analysis_meta.get('job_timeout')}")

            job_timeout = analysis_meta.get('job_timeout', "10800")
            priority = analysis_meta.get('priority', 0)
            if priority == 1:
                high_priority = True
            else:
                high_priority = False

            #job_yaml = make_cellprofiler_yaml(cellprofiler_version, pipeline_file, imageset_file, output_path, job_name, analysis_id, sub_analysis_id, job_timeout, high_priority)
            cellprofiler_cmd = get_cellprofiler_cmd_uppmax(cellprofiler_version, pipeline_file, imageset_file, output_path, job_name, analysis_id, sub_analysis_id, job_timeout, high_priority)

            #§§logging.info(f"cellprofiler_cmd {cellprofiler_cmd}")

            # Check if icf headers should be added to imgset csv file, default is False
            use_icf = analysis_meta.get('use_icf', False)
            logging.debug("use_icf" + str(use_icf))
            # generate cellprofiler imgset file for this imgset
            logging.debug(f"channel_map: {channel_map}")
            imageset_content = make_imgset_csv(imgsets=imgset_chunk, channel_map=channel_map, storage_paths=storage_paths, use_icf=use_icf)

            logging.debug(f"imgset: {imageset_content[:2000]}")

            # create a folder for the file if needed
            os.makedirs(os.path.dirname(imageset_file), exist_ok=True)
            # write csv
            with open(imageset_file, 'w') as file:
                file.write(imageset_content)

            all_cmds.append(cellprofiler_cmd)

            if job_limit is not None and i >= (job_limit-1):
                print("exit here")
                break

            if is_debug():
                logging.error("Is debug, exit here")
                exit()

        # when all chunks of the sub analysis are sent in, mark the sub analysis as started
        #mark_analysis_as_started(cursor, connection, analysis['analysis_id'])
        #mark_sub_analysis_as_started(cursor, connection, analysis['sub_id'])


        sub_analysis_out_dir = f"/cpp_work/input/{sub_analysis_id}"
        with open(f"{sub_analysis_out_dir}/cmds.txt", "w") as file:
            for item in all_cmds:
                file.write(item + "\n")

        #logging.info(f"all_smds: {all_cmds}")

        sub_type = analysis_meta.get('sub_type', "undefifed")
        logging.debug("sub_type" + str(use_icf))

        job_id = submit_sbatch_to_uppmax(sub_analysis_id, sub_type, analysis_id)

        if job_id:
            update_sub_analysis_status_to_db(connection, cursor, analysis_id, sub_analysis_id, f"submitted, job_id={job_id}")

            # when all chunks of the sub analysis are sent in, mark the sub analysis as started
            mark_analysis_as_started(cursor, connection, analysis['analysis_id'])
            mark_sub_analysis_as_started(cursor, connection, analysis['sub_id'])


def submit_sbatch_to_uppmax(sub_id, sub_type, analysis_id):
    return submit_sbatch_to_rackham(sub_id, sub_type, analysis_id)

def submit_sbatch_to_rackham(sub_id, sub_type, analysis_id):

    logging.info(f"inside submit_sbatch_to_uppmax: {sub_id}, sub_type {sub_type}")

    job_id = None

    max_errors = 10

    if sub_type == "icf":
        nHours = 20 # it should only take about 10h, but I have noticed image load problems when running 20+ icf in parallell
        nTasks = 1
        partition = "core"
        workers = 1
    elif sub_type == "qc":
        nHours = 20
        nTasks = 20
        partition = "node"
        workers = 20
    else:
        nHours = 100
        nTasks = 20
        partition = "node"
        workers = 20

    cpp_config = load_cpp_config()
    project_id = cpp_config['uppmax_project_rackham']

    # Define your command as a string
    cmd = (f"ssh -o StrictHostKeyChecking=no"
           f" {cpp_config['uppmax_user']}@{cpp_config['uppmax_hostname']}"
           f" sbatch"
	       f" -n {nTasks}"
	       f" -t {nHours}:00:00"
           f" --job-name=cpp_{analysis_id}_{sub_id}_{sub_type}"
	       f" --output=logs/{analysis_id}_{sub_id}-slurm.%j.out"
           f" --error=logs/{analysis_id}_{sub_id}-slurm.%j.out"
	       f" -A {project_id}"
	       f" -D /home/andersl/cpp_uppmax"
	       f" run_cellprofiler_apptainer.sh"
	       f" -d /cpp_work/input/{sub_id}"
           f" -o /cpp_work/output/{sub_id}"
           f" -w {workers}"
           f" -e {max_errors}"
        )

    logging.info(f"cmd: {cmd}")

    try:
        # Execute the command and capture the output
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        logging.info(f"stdout: {output}")

        # Define a regular expression pattern to match the batch job ID
        pattern = r'Submitted batch job (\d+)'

        match = re.search(pattern, output)
        if match:
            job_id = match.group(1)
        else:
            logging.error(f"No batch job ID found in the std out, stdout: {output}")

    except subprocess.CalledProcessError as e:
        error_message = f"Error: {e.returncode}\n{e.output}"
        logging.error(error_message)

    return job_id



def submit_sbatch_to_snowy(sub_id, sub_type, analysis_id):

    logging.info(f"inside submit_sbatch_to_uppmax: {sub_id}, sub_type {sub_type}")

    job_id = None

    max_errors = 10

    if sub_type == "icf":
        nHours = 30 # it should only take about 10h, but I have noticed image load problems when running 20+ icf in parallell
        nNodes = 1
    elif sub_type == "qc":
        nHours = 60
        nNodes = 16
    else:
        nHours = 130
        nNodes = 16

    cpp_config = load_cpp_config()
    project_id = cpp_config['uppmax_project_snowy']

    # Define your command as a string
    cmd = (f"ssh -o StrictHostKeyChecking=no"
           f" {cpp_config['uppmax_user']}@{cpp_config['uppmax_hostname']}"
           f" sbatch"
	       f" -M snowy"
	       f" -n {nNodes}"
	       f" -t {nHours}:00:00"
           f" --job-name=cpp_{analysis_id}_{sub_id}_{sub_type}"
	       f" --output=logs/{analysis_id}_{sub_id}-slurm.%j.out"
           f" --error=logs/{analysis_id}_{sub_id}-slurm.%j.out"
	       f" -A {project_id}"
	       f" -D /home/andersl/cpp_uppmax"
	       f" run_cellprofiler_apptainer.sh"
	       f" -d /cpp_work/input/{sub_id}"
           f" -o /cpp_work/output/{sub_id}"
           f" -w {nNodes}"
           f" -e {max_errors}"
        )

    logging.info(f"cmd: {cmd}")

    try:
        # Execute the command and capture the output
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, text=True)
        logging.info(f"stdout: {output}")

        # Define a regular expression pattern to match the batch job ID
        pattern = r'Submitted batch job (\d+)'

        match = re.search(pattern, output)
        if match:
            job_id = match.group(1)
        else:
            logging.error(f"No batch job ID found in the std out, stdout: {output}")

    except subprocess.CalledProcessError as e:
        error_message = f"Error: {e.returncode}\n{e.output}"
        logging.error(error_message)

    return job_id

def get_joblist():
    # list all jobs in namespace
    k8s_batch_api = kubernetes.client.BatchV1Api()
    job_list = k8s_batch_api.list_namespaced_job(namespace=get_namespace())
    return job_list

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
        logging.debug("Queue is not empty, exit loop")
        break

    logging.info("Finished is_kubernetes_job_queue_empty, is_queue_empty=:" + str(is_queue_empty))
    return is_queue_empty

def delete_finished_jobpods():
    logging.info("inside delete_finished_jobpods")
    namespace = get_namespace()
    core = kubernetes.client.CoreV1Api()

    # Bulk‐delete every Pod in Succeeded phase
    core.delete_collection_namespaced_pod(
        namespace=namespace,
        field_selector="status.phase=Succeeded",
        grace_period_seconds=15,
        propagation_policy="Background"  # fire-and-forget
    )

    logging.info("done delete_finished_jobpods")



def delete_finished_jobpods_old():
    logging.info("inside delete_finished_jobpods")
    namespace = get_namespace()
    k8s_batch_api = kubernetes.client.BatchV1Api()
    k8s_core_api = kubernetes.client.CoreV1Api()

    job_list = k8s_batch_api.list_namespaced_job(namespace=namespace)

    for job in job_list.items:

        finished = job.status.completion_time

        if finished:

            job_name = job.metadata.name
            label_selector = f"job-name={job_name}"

            pods = k8s_core_api.list_namespaced_pod(namespace, label_selector=label_selector)
            for pod in pods.items:
                logging.debug(f"delete pod: {pod.metadata.name}")
                k8s_core_api.delete_namespaced_pod(pod.metadata.name, namespace, propagation_policy="Background")

    logging.info("done delete_finished_jobpods")

def fetch_finished_job_families(cursor, connection, job_limit = None):
    logging.info("Inside fetch_finished_job_families")

    # list all jobs in namespace
    k8s_batch_api = kubernetes.client.BatchV1Api()
    job_list = k8s_batch_api.list_namespaced_job(namespace=get_namespace())

    # filter out all finished jobs
    finished_jobs = {}
    for job in job_list.items:

        # convert to dict for usability
        job_dict = job.to_dict()

        conditions = job_dict['status'].get('conditions')
        if not conditions:
            continue

        condition_types = [c.get('type') for c in conditions]
        logging.debug(f"Job {job_dict['metadata']['name']} conditions: {condition_types}")

        is_complete = any(c.get('type') == 'Complete' and c.get('status') == 'True' for c in conditions)
        is_failed = any(c.get('type') == 'Failed' and c.get('status') == 'True' for c in conditions)

        if is_complete:
            finished_jobs[job_dict['metadata']['name']] = job_dict
        elif is_failed:
            handle_sub_analysis_error(cursor, connection, job_dict)
            finished_jobs[job_dict['metadata']['name']] = job_dict

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
        #── update meta.progress in DB ──
        # both analysis_id & sub_analysis_id live in metadata.labels
        labels = job_list[0]['metadata'].get('labels', {})
        try:
            analysis_id     = int(labels['analysis_id'])
            sub_analysis_id = int(labels['sub_analysis_id'])
            # record “done / total” + ETA
            update_progress(connection, cursor,
                            analysis_id,
                            sub_analysis_id,
                            done=len(job_list),
                            total=family_job_count)
        except (KeyError, ValueError):
            # if labels aren’t there, we just skip progress update
            logging.debug("Could not update progress: missing labels on job")

        # check if there are as many finished jobs as the total job count for the family
        # for debug reasons we also check if the job limit is reached
        if family_job_count == len(job_list) or (job_limit is not None and len(job_list) == job_limit):

            # then all jobs in this family are finished and ready to be processed
            finished_families[family_name] = job_list

        # update progress
        #done = len(job_list)
        #total = family_job_count
        #sub_id = job_list[0]['metadata']['sub_id']
        #analysis_id = job_list[0]['metadata']['analysis_id']
        #update_progress(connection, cursor, analysis_id, sub_id, done, total)


    logging.debug("Finished families: " + str(len(finished_families)))
    return finished_families

def get_all_dirs(path):
    # Create a Path object from the given path
    path_obj = pathlib.Path(path)

    # Check if the path exists and is a directory
    if not path_obj.exists() or not path_obj.is_dir():
        # Return None if the path is not a directory or does not exist
        return None

    # Initialize an empty list to hold the names of all directories
    dir_names = []

    # Iterate over each item in the directory
    for d in path_obj.iterdir():
        # Check if the item is a directory
        if d.is_dir():
            # Add the directory name to the list
            dir_names.append(d.name)

    # Return the list of directory names
    return dir_names


def fetch_finished_job_families_uppmax(cursor, connection, job_limit = None):
    logging.info("Inside fetch_finished_job_families_uppmax")

    sql = (f"""
            SELECT *
            FROM image_sub_analyses
            WHERE finish IS null
            AND error IS null
            AND meta->>'run_on_uppmax' IS NOT null
            """)

    logging.debug("sql" + sql)

    cursor.execute(sql)

    unfinished_sub_analyses = cursor.fetchall()

    finished_jobs = {}
    for sub_analysis in unfinished_sub_analyses:
        sub_id = sub_analysis['sub_id']
        analysis_id = sub_analysis['analysis_id']
        logging.debug(f"sub_analyses: {sub_id}")

        sub_analysis_out_path = f"/cpp_work/output/{sub_id}"
        logging.debug(f'sub_analysis_out_path {sub_analysis_out_path}')

        # list all jobs in output
        if os.path.exists(sub_analysis_out_path):
            all_sub_analyses_jobs = get_all_dirs(sub_analysis_out_path)
            logging.debug(f'len(all_sub_analyses_jobs) {len(all_sub_analyses_jobs)}')
            for job in all_sub_analyses_jobs:

                job_path = os.path.join(sub_analysis_out_path, job)

                #logging.info(f'job_path {job_path}')

                if os.path.exists(os.path.join(job_path, "error")):
                    logging.debug(f"Error job {job}")
                    finished_jobs[job] = {"metadata": {"name": job, "sub_id": sub_id, "analysis_id": analysis_id}}
                elif os.path.exists(os.path.join(job_path, "finished")):
                    finished_jobs[job] = {"metadata": {"name": job, "sub_id": sub_id, "analysis_id": analysis_id}}
                    logging.debug(f"Job finished: {job}")

            logging.debug(f"Finished jobs after this sub {str(len(finished_jobs))}")


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
        logging.debug(f"fam-job-count: {family_job_count}\tfinished-job-list-len: {len(job_list)}")
        # check if there are as many finished jobs as the total job count for the family
        # for debug reasons we also check if the job limit is reached
        if family_job_count == len(job_list) or (job_limit is not None and len(job_list) == job_limit):

            # then all jobs in this family are finished and ready to be processed
            finished_families[family_name] = job_list


        # update progress
        done = len(job_list)
        total = family_job_count
        sub_id = job_list[0]['metadata']['sub_id']
        analysis_id = job_list[0]['metadata']['analysis_id']
        update_progress(connection, cursor, analysis_id, sub_id, done, total)



    logging.info("Finished families: " + str(len(finished_families)))
    return finished_families

def update_progress(connection, cursor, analysis_id, sub_id, done, total):

    start_time = get_sub_analysis_start(connection, cursor, sub_id)
    if not start_time:
        start_time = datetime.datetime.now()
    # Calculate elapsed time and average time per job
    elapsed_time = datetime.datetime.now() - start_time
    average_time_per_job = elapsed_time.total_seconds() / done
    jobs_remaining = total - done
    time_remaining = average_time_per_job * jobs_remaining
    hours_remaining = time_remaining / 3600  # Convert seconds to hours
    progress = f"{done} / {total} jobs finished, time remaining: {hours_remaining:.2f} hours"

    update_sub_analysis_progress_to_db(connection, cursor, analysis_id, sub_id, progress)


# goes through all jobs of a family i.e. merges the csvs with the same names into
# a single resulting csv file for the entire family, e.g. ..._Experiment.csv, ..._Image.csv
def merge_family_jobs_csv(family_name, job_list):

    logging.info("Inside merge_family_jobs_csv")

    logging.debug("job_list:" + str(job_list))

    # init
    merged_csvs = {}

    # for each job in the family
    for job in job_list:

        # fetch all csv files in the job folder
        analysis_sub_id = get_analysis_sub_id_from_family_name(family_name)
        job_path = f"/cpp_work/output/{analysis_sub_id}/{job['metadata']['name']}"
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

    logging.info("done merge_family_jobs_csv")

    return merged_csvs

def to32bit(t):
    return t.astype({c: str(t[c].dtype).replace('64', '32') for c in t.columns})

# goes through all jobs of a family i.e. merges the csvs with the same names into
# a single resulting csv file for the entire family, e.g. ..._Experiment.csv, ..._Image.csv
def merge_family_jobs_csv_to_parquet(family_name, cursor, connection):

    logging.info("Inside merge_family_jobs_csv_to_parquet")

    # find all csv files in the sub-analayses folder
    analysis_sub_id = get_analysis_sub_id_from_family_name(family_name)
    sub_analysis_path = f"/cpp_work/output/{analysis_sub_id}/"

    # Put csv-files in dict of lists where dict-key is csv-filename (all files have same name
    # but are in different sub-dirs (job-dirs))
    all_csv_files = pathlib.Path(sub_analysis_path).rglob("*.csv")
    filename_dict = {}
    for file in all_csv_files:
        filename = os.path.basename(file)
        file_list = filename_dict.setdefault(filename, [])
        file_list.append(file)

    # some files should not be concatenated but only one file should be copied
    # They are being put here into a separate dict and then one file is renemed to another extension than csv
    excludes = ["qcRAW_experiment_", "icf_experiment_" "_experiment.csv", 'Experiment.csv']
    filename_excluded = {}
    for exclude in excludes:
        for key in list(filename_dict.keys()):
            if exclude in key:
                filename_excluded[key] = filename_dict[key]
                del filename_dict[key]

    # concat all csv-files (per filename), loop filename(key)
    for filename in filename_dict.keys():

        start = time.time()

        files = filename_dict[filename]

        # create concat-csv with all files with current filename, e.g experiment, nuclei, cytoplasm
        is_header_already_included = False
        tmp_csvfile = os.path.join('/tmp/', filename + '.merged.csv.tmp')
        try:
            with open(tmp_csvfile, 'w') as csvout:
                for fileCount, file in enumerate(files):
                    with open(file, "r") as f:
                        # only include header once
                        if is_header_already_included:
                            next(f)
                        for row in f:
                            csvout.write(row)
                            is_header_already_included = True

                    if fileCount % 500 == 0:
                        logging.info(f'{fileCount}/{len(files)} {filename}')


            logging.info(f'done concat csv {filename}')
            logging.info(f"elapsed: {(time.time() - start):.3f}")
            logging.info(f'start pd.read_csv {tmp_csvfile}')
            pyarrow.set_cpu_count(5)
            #parse_options
            df = pd.read_csv(tmp_csvfile, engine="c")
            os.remove(tmp_csvfile)
            logging.info(f'done concat {filename}')
            logging.info(f"elapsed: {(time.time() - start):.3f}")
            logging.info(f'start save as parquet {filename}')
            df = to32bit(df)
            parquetfilename = os.path.splitext(filename)[0] + '.parquet'
            parquetfile = os.path.join(sub_analysis_path, parquetfilename)
            df.to_parquet(parquetfile, engine="pyarrow")
            logging.info(f'done save as parquet {parquetfile}')
            logging.info(f"elapsed: {(time.time() - start):.3f}")

        except Exception as e:
            errormessage = f"Failed during concat csv files, error {e}"
            logging.error("Failed during concat csv files, error: %s", e)
            analysis_id = get_analysis_sub_id_from_family_name(family_name)
            set_sub_analysis_error(cursor, connection, analysis_id, analysis_sub_id, errormessage)
                    # delete all jobs for this sub_analysis
            delete_jobs(analysis_sub_id)

        finally:
            if os.path.exists(tmp_csvfile):
                os.remove(tmp_csvfile)

    logging.info("done merge_family_jobs_csv_to_parquet")



# goes through all the non-csv filescsv of a family of job and copies the result to the result folder
def move_job_results_to_storage(family_name, job_list, storage_root):

    logging.info("Inside move_job_results_to_storage")

    skip_suffixes = ['.csv', '.log']
    skip_files = ['finished', 'error']

    files_created = []

    # for each job in the family
    for job in job_list:

        # fetch all files in the job folder
        analysis_sub_id = get_analysis_sub_id_from_family_name(family_name)
        job_path = f"/cpp_work/output/{analysis_sub_id}/{job['metadata']['name']}"
        for result_file in pathlib.Path(job_path).rglob("*"):

            logging.debug("move file: " + str(result_file))

            # exclude files with these extensions
            if result_file.suffix in skip_suffixes or result_file.name in skip_files or pathlib.Path.is_dir(result_file):
                logging.debug("Skipping file: " + str(result_file))
                continue

            # keep only the path relative to the job_path
            filename = str(result_file).replace(job_path+'/', '')

            # create a folder for the file if needed
            subdir_name = os.path.dirname(filename)
            os.makedirs(f"{storage_root['full']}/{subdir_name}", exist_ok=True)

            # move the file to the storage location
            shutil.move(f"{job_path}/{filename}", f"{storage_root['full']}/{filename}")

            # remember the file
            files_created.append(f"{filename}")

            logging.debug("done move file: " + str(filename))

    # move the concatenated output-csv that are in parquet format in sub-analysis dir
    sub_analysis_path = f"/cpp_work/output/{analysis_sub_id}/"
    for result_file in pathlib.Path(sub_analysis_path).glob("*.parquet"):

        # keep only the filename in result
        filename = pathlib.Path(result_file).name

        # create a folder for the file if needed
        subdir_name = os.path.dirname(filename)
        os.makedirs(f"{storage_root['full']}/{subdir_name}", exist_ok=True)

        # move the file to the storage location
        shutil.move(f"{result_file}", f"{storage_root['full']}/{filename}")

        # remember the file
        files_created.append(f"{filename}")

        logging.debug("done copy file: " + str(filename))


    logging.info("Done move_job_results_to_storage")

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

def get_analysis_id_from_family_name(family_name):
    match = re.match('cpp-worker-job-\d+-\w+-\d+-\d+-(\d+)', family_name)
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

def get_analysis_from_db(cursor, analysis_id):

    # fetch all images belonging to the plate acquisition
    logging.info('Fetching plate info from view.')
    query = f"""
                        SELECT *
                        FROM image_analyses_v1
                        WHERE id='{analysis_id}'
                       """ # also NOT IN (select * from images_analysis where analysed=None) or something

    logging.debug(query)
    cursor.execute(query)
    analysis = cursor.fetchone()

    return analysis


def get_middle_z_plane(cursor, acq_id):
    logging.info(f'Fetching distinct z-plane values for plate acquisition ID: {acq_id}')
    
    # Get a sorted list of unique z values
    query = """
            SELECT DISTINCT z
            FROM images
            WHERE plate_acquisition_id = %s
            ORDER BY z ASC
            """
    logging.info('Executing query: %s with ID: %s', query.strip(), acq_id)
    cursor.execute(query, (acq_id,))
    results = cursor.fetchall()
    
    # Create a list of unique z values
    z_values = [row['z'] for row in results]
    
    if not z_values:
        logging.warning('No distinct z values found for plate acquisition ID: %s', acq_id)
        return None
    
    # Determine the middle index (using lower middle for even counts)
    middle_index = (len(z_values) - 1) // 2
    logging.debug(f"Total unique z values: {len(z_values)}. Middle index: {middle_index}")
    
    return z_values[middle_index]


def get_first_z_plane(cursor, acq_id):
    # Log the start of fetching data
    logging.info(f'Fetching minimum z-plane for plate acquisition ID: {acq_id}')

    # SQL query to select the minimum z value where the plate_acquisition_id matches
    query = """
            SELECT min(z) AS min_z
            FROM images
            WHERE plate_acquisition_id = %s
            """
    # Log the query string
    logging.info('Executing query: %s with ID: %s', query.strip(), acq_id)

    # Execute the SQL query with the provided acquisition ID
    cursor.execute(query, (acq_id,))

    # Fetch the result as a dictionary
    result = cursor.fetchone()

    return result['min_z']


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

def update_sub_analysis_errormsg_to_db(connection, cursor, analysis_id, sub_analysis_id, errormessage):
    # TODO first get current errormessage, then append id
    update_status_data_to_db(connection, cursor, analysis_id, sub_analysis_id, "error", errormessage)

def update_sub_analysis_status_to_db(connection, cursor, analysis_id, sub_analysis_id, status):
    update_status_data_to_db(connection, cursor, analysis_id, sub_analysis_id, "status", status)

def update_sub_analysis_progress_to_db(connection, cursor, analysis_id, sub_analysis_id, progress):
    update_status_data_to_db(connection, cursor, analysis_id, sub_analysis_id, "progress", progress)

def update_status_data_to_db(connection, cursor,
                             analysis_id: int,
                             sub_analysis_id: int,
                             data_key: str,
                             data_value: str):
    """
    Merge a single key/value into the JSONB `status` column
    of both image_sub_analyses and image_analyses.
    """
    logging.debug(f"Updating status[{data_key}] for sub_id={sub_analysis_id}")

    # Merge into image_sub_analyses.status, initializing to {} if NULL
    sub_q = """
      UPDATE image_sub_analyses
         SET status = COALESCE(status, '{}'::jsonb) || %s
       WHERE sub_id = %s
    """
    sub_data = {data_key: data_value}
    cursor.execute(sub_q, [psycopg2.extras.Json(sub_data), sub_analysis_id])

    # Merge into image_analyses.status
    # Use a distinct key so multiple sub-analyses don't collide
    parent_q = """
      UPDATE image_analyses
         SET status = COALESCE(status, '{}'::jsonb) || %s
       WHERE id = %s
    """
    parent_data = {f"{data_key}_{sub_analysis_id}": data_value}
    cursor.execute(parent_q, [psycopg2.extras.Json(parent_data), analysis_id])

    connection.commit()



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


    # Filter file list (remove individual png/tif files and only save path....)
    result['file_list'] = filter_list_remove_imagefiles(result['file_list'])

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

    delete_jobs(sub_analysis_id)

def filter_list_remove_imagefiles(list):
     suffix = ('.png','.jpg','.tiff','.tif')
     return filter_list_remove_files_suffix(list, suffix)

def filter_list_remove_files_suffix(input_list, suffix):

    filtered_list = []
    was_filtered = False
    for file in input_list:
        if file.lower().endswith(suffix):
            # remove filename and add path only to filtered list
            filtered_list.append(os.path.dirname(file) + '/')
            was_filtered = True
        else:
            filtered_list.append(file)

    unique_filtered_list = list(set(filtered_list))

    if was_filtered:
        logging.debug("unique_filtered_list" + str(unique_filtered_list))

    return unique_filtered_list



# go through unfinished analyses and wrap them up if possible (check if sub-analyses belonging to them are all finished)
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

                # if so, mark the analysis as failed
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

            set_analysis_error(analysis['id'], cursor, connection )


def set_analysis_error(analysis_id, cursor, connection):
    logging.info(f"inside set_analysis_error analysis_id {analysis_id}")
    # create timestamp
    error_time = str(datetime.datetime.now())

    # construct query
    query = f""" UPDATE image_analyses
                        SET error=%s
                        WHERE id=%s
            """
    cursor.execute(query, [error_time, analysis_id])
    connection.commit()

def delete_jobs(sub_analysis_id, leave_failed=True):

    namespace = get_namespace()
    logging.debug('Inside delete_job')

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

            # if the has been marked failed leave it for debugging
            if (job_dict['status']['conditions'] and
                job_dict['status']['conditions'][0]['type'] == 'Failed' and
                leave_failed):
                continue # do nothing
            else:
                response = k8s_batch_api.delete_namespaced_job(job_name, namespace, propagation_policy='Background') # background is also possible, no idea about difference



sub_anal_err_count = {}
job_error_set = set()
def handle_sub_analysis_error(cursor, connection, job):

    job_name = job['metadata']['name']

    # only deal with error once
    if job_name in job_error_set:
        return
    else:
        job_error_set.add(job_name)

    # get sub analysis id
    sub_analysis_id = get_analysis_sub_id_from_family_name(job_name)

    # get analysis id
    analysis_id = get_analysis_id_from_family_name(job_name)

    # increment error count for this sub analysis
    sub_anal_err_count[str(sub_analysis_id)] = sub_anal_err_count.get(str(sub_analysis_id), 0) + 1

    error_count = sub_anal_err_count[str(sub_analysis_id)]
    logging.info("error_count: " + str(error_count))

    # get max_errors for this sub-analysis
    max_errors = get_sub_analysis_max_errors(cursor, connection, sub_analysis_id)

    if error_count > max_errors:
        logging.info(f"error_count is more than max_errors")

        # Check if failed already there
        if not has_sub_analysis_error(cursor, connection, sub_analysis_id):
            errormessage = f"error_count is more than max_errors"
            set_sub_analysis_error(cursor, connection, analysis_id, sub_analysis_id, errormessage)
            add_error_message_to_sub_analysis(cursor, connection, analysis_id, sub_analysis_id, errormessage)

        # delete all jobs for this sub_analysis
        delete_jobs(sub_analysis_id)
    else:
        errormessage = f"error in job {job_name}"
        add_error_message_to_sub_analysis(cursor, connection, analysis_id, sub_analysis_id, errormessage)

    logging.info("done with handle_sub_analysis_error")

def add_error_message_to_sub_analysis(cursor, connection, analysis_id, sub_analysis_id, errormessage):
    update_sub_analysis_errormsg_to_db(connection, cursor, analysis_id, sub_analysis_id, errormessage)


def set_sub_analysis_error(cursor, connection, analysis_id, sub_analysis_id, errormessage="no error message"):

    add_error_message_to_sub_analysis(cursor, connection, analysis_id, sub_analysis_id, errormessage)

    # Set error in sub analyses
    query = """ UPDATE image_sub_analyses
                        SET error=%s
                        WHERE sub_id=%s
            """
    cursor.execute(query, [str(datetime.datetime.now()), sub_analysis_id,])
    connection.commit()

    set_analysis_error(analysis_id, cursor, connection)


def has_sub_analysis_error(cursor, connection, sub_analysis_id):
    """
    Checks if a sub-analysis has an error.

    Parameters:
    - cursor: The database cursor.
    - connection: The database connection (not used here, but kept for context).
    - sub_analysis_id: The ID of the sub-analysis to check.

    Returns:
    - True if the sub-analysis has an error, False otherwise.
    - None if the sub-analysis does not exist.
    """

    # Query to check for errors in the sub-analysis
    query = """SELECT error FROM image_sub_analyses
               WHERE sub_id = %s"""

    # Log the fully expanded SQL statement
    logging.info("Mogrified SQL: %s", cursor.mogrify(query, [sub_analysis_id]))

    cursor.execute(query, [sub_analysis_id])
    result = cursor.fetchone()

    if result is None:
        # Sub-analysis ID not found in the table
        return True
    else:
        # Check if the error column indicates an error
        has_error = result["error"]
        return bool(has_error)

def get_sub_analysis_start(connection, cursor, sub_id):
    query = """ SELECT start FROM image_sub_analyses
                WHERE sub_id=%s
            """

    cursor.execute(query, [sub_id,])

    row = cursor.fetchone()

    if row:
        start = row['start']
        start = start.replace(tzinfo=None)
    else:
        start = None

    return start

def get_sub_analysis_max_errors(cursor, connection, sub_analysis_id):

    query = """ SELECT meta->>'max_errors' AS max_errors FROM image_sub_analyses
                WHERE sub_id=%s
    """

    cursor.execute(query, [sub_analysis_id,])
    row = cursor.fetchone()

    if row and row['max_errors']:
        max_errors = row['max_errors']
    else:
        max_errors = 0

    return int(max_errors)


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

    return get_storage_paths(plate_barcode, acquisition_id, analysis_id)

def get_storage_paths_from_sub_analysis_id(cursor, sub_analysis_id):

    logging.info("Inside get_storage_paths_from_sub_analysis_id")

    analysis_info = get_sub_analysis_info(cursor, sub_analysis_id)

    plate_barcode = analysis_info["plate_barcode"]
    acquisition_id = analysis_info["plate_acquisition_id"]
    analysis_id =  analysis_info["analyses_id"]

    return get_storage_paths(plate_barcode, acquisition_id, analysis_id)

def get_storage_paths(plate_barcode, acquisition_id, analysis_id):
    storage_paths = {
        "full": f"/cpp_work/results/{plate_barcode}/{acquisition_id}/{analysis_id}",
        "mount_point":"/cpp_work/",
        "job_specific":f"results/{plate_barcode}/{acquisition_id}/{analysis_id}/"
        }
    return storage_paths

def setup_logging(log_level):
        # set up logging to file
        now = datetime.datetime.now()
        now_string = now.strftime("%Y-%m-%d_%H.%M.%S.%f")

        print(f"is_debug {is_debug()}")

        if log_level == logging.DEBUG:
            log_prefix = "debug."
        else:
            log_prefix = ""

        logfile_name = "/cpp_work/logs/cpp_master." + log_prefix + now_string + ".log"

        logging.basicConfig(format='%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                            datefmt='%Y-%m-%d:%H:%M:%S',
                            level=log_level,
                            filename=logfile_name,
                            filemode='w')

        # define a Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(log_level)

        # set a formater for console
        consol_fmt = logging.Formatter('%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
        # tell the handler to use this format
        console.setFormatter(consol_fmt)

        # add the handler to the root logger
        logging.getLogger('').addHandler(console)

def merge_and_move(family_name, job_list, cursor, connection):
    """
    Merge a family’s CSVs into Parquet, move the results into storage,
    and insert the final file list into the DB.
    """
    sub_id = get_analysis_sub_id_from_family_name(family_name)
    storage_paths = get_storage_paths_from_sub_analysis_id(cursor, sub_id)

    # 1) Merge into Parquet
    merge_family_jobs_csv_to_parquet(family_name, cursor, connection)

    # 2) Move into final storage
    files_created = move_job_results_to_storage(family_name, job_list, storage_paths)

    # 3) Record results in DB
    insert_sub_analysis_results_to_db(connection, cursor, sub_id, storage_paths, files_created)


# a global executor (1 thread) and a lock set
merge_executor = ThreadPoolExecutor(max_workers=1)
in_processing_families = set()
in_processing_families_lock = threading.Lock()

def process_finished_families(finished_families, cursor, connection):
    """
    Throttle merging/moving of finished_families (max 1 at a time),
    ensure each family is only worked on once, and record results.

    Each worker thread opens its own DB connection so it doesn't
    end up using a cursor closed by the main loop.
    """
    def worker(family, jobs):
        try:
            cpp_config = load_cpp_config()
            conn, cur = connect_db(cpp_config)
            try:
                merge_and_move(family, jobs, cur, conn)
            finally:
                cur.close()
                conn.close()
        except Exception:
            logging.exception(f"Error in merge_and_move for family {family}")
        finally:
            # now that we’re truly done, free the slot
            with in_processing_families_lock:
                in_processing_families.remove(family)

    # 1) decide which families to submit (not already in flight)
    with in_processing_families_lock:
        to_submit = {
            fam: jobs
            for fam, jobs in finished_families.items()
            if fam not in in_processing_families
        }
        # 2) mark them *before* submitting, so repeated ticks won't re-submit
        for fam in to_submit:
            in_processing_families.add(fam)

    # 3) submit them and drop from finished_families
    for fam, jobs in to_submit.items():
        merge_executor.submit(worker, fam, jobs)


def main():

    try:

        log_level = logging.DEBUG if is_debug() else logging.INFO
        setup_logging(log_level)

        # set file permissions on ssh key
        #os.chmod('/root/.ssh/id_rsa', 0o600)

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

                delete_finished_jobpods()

                handle_new_jobs(cursor, connection, job_limit = job_limit)

                finished_families = fetch_finished_job_families(cursor, connection, job_limit = job_limit)
                finished_families_uppmax = fetch_finished_job_families_uppmax(cursor, connection, job_limit = job_limit)
                finished_families.update(finished_families_uppmax)

                # merge and move finised jobs for each family (i.e. merge jobs for a sub analysis)
                process_finished_families(finished_families, cursor, connection)

                # check for finished analyses
                handle_finished_analyses(cursor, connection)

                # update hpc status
                hpc_utils.update_hpc_job_status(Database.get_instance(), cpp_config['uppmax']['user'], cpp_config['uppmax']['hostname'])


            # Catch psycopg2 database errors
            except psycopg2.Error as error:
                logging.exception("Database error")


            # Catch value errors
            except ValueError as valerror:
                logging.exception("Value error: ")

            finally:
                #closing database connection
                if connection:
                    cursor.close()
                    connection.close()


            #print('Exit because single run debug')
            #exit()

            sleeptime = 20
            logging.info(f"Going to sleep for {sleeptime} sec")
            time.sleep(sleeptime)

    # Catch all errors
    except Exception as e:
        logging.error("Exception: %s", e)


if __name__ == "__main__":


    main()






