# Continiopus Cellpainting 

## TODO cpp_master.py
* get channel_map into the database
* get plate acqusitions into the database
OK * fetch db login info from secret
* fetch only images that have not been analysed from a plate acqusition?
* store the imgset file as a configmap for each job?
* fix the job spec yaml, the command and mount paths (root vs user etc)
* make sure the worker container image exists and works

## TODO cluster
OK * Generate secret with db login

## Setup cluster for cpp pipeline
OK * Create namespace cppipeline in cluster
OK * Create mikro pv/pvc in namespace cppipeline
OK * Create rancher user in namespace cppipeline
OK * Get rancher user .kube/config-file
OK * Generate kube config secret:
OK   * `kubectl delete secret -n cppipeline cppipipeline-kube-conf`
OK   * `kubectl create secret generic cppipipeline-kube-conf -n cppipeline --from-file=kube-config`
