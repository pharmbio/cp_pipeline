# Continiopus Cellpainting 

## TODO cpp_master.py
* get channel_map into the database
* get plate acqusitions into the database
* fetch db login info from secret
* fetch only images that have not been analysed from a plate acqusition?
* store the imgset file as a configmap for each job?
* fix the job spec yaml, the command and mount paths (root vs user etc)
* make sure the worker container image exists and works

## TODO cluster
* Generate secret with db login

## Setup cluster for cpp pipeline
* Create namespace cppipeline in cluster
* Create mikro pv/pvc in namespace cppipeline
* Create rancher user in namespace cppipeline
* Get rancher user .kube/config-file
* Generate kube config secret:
* * kubectl delete secret -n cppipeline cppipipeline-kube-conf
* * kubectl create secret generic cppipipeline-kube-conf -n cppipeline --from-file=kube-config
