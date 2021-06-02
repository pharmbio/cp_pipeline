# Continuous Cellpainting - Cellprofiler pipelines automation on Kubernetes

![cp_pipeline overview](analysis_pipeline_overview.png)


Cellprofiler pipelines examples:

- QC.cppipe
- Illumination_correction.cppipe
- FindFeatures.cppipe

Few cellprofiler modules are multithreaded

## Dahlö-cppipeline:

- Split plate into a few wells (batches)
- Parallellize on Kubernetes (Relying on Job names and Kubernetes Scheduler to keep track of finished batches and analyses)
- Cat individual results into large table

### Method:

- cp_pipeline_master.py (Python script running in a pod on cluster, reading data from Postgres Imagedb):
- some new tables in Imagedb (https://imagedb-adminer.k8s-prod.pharmb.io/?pgsql=imagedb-pg-postgresql.services.svc.cluster.local&username=postgres&db=imagedb&ns=public)
  - For each new analyses, split into batches and for each batch create a Kubernetes Job Yaml
  - Throw all job yamls onto Kubernetes Cluster (Hundreds or Thousands)
  - Limit jobs concurrent running with Kubernetes Namespace Quotas
  - Let Kubernetes Scheduler start new job-pods when resources are available
 
### Directories:
```
/share/data/cellprofiler/automation/pipelines
/share/data/cellprofiler/automation/work
/share/data/cellprofiler/automation/results

/share/mikro/ ( 8.6TB images )
```

## Pipeline Gui: https://pipelinegui.k8s-prod.pharmb.io/

- A GUI for inserting Analyses definitions into the Postgres ImageDB


## Future: 
  Visualize image feature results in ImageDB-Gui (https://imagedb.k8s-prod.pharmb.io/) Exp3 Vero Zika L2
  

# Usage
```
# Start worker
cd worker
kubectl apply -f deployment.yaml

# Log in to worker for debugging
kubectl exec -it -n cppipeline <name of pod> -- bash
```

## TODO cpp_master.py
* get channel_map into the database
* get plate acqusitions into the database (for now it is a table, in future maybe view)
* OK * fetch db login info from secret
* fetch only images that have not been analysed from a plate acqusition?
* store the imgset file as a configmap for each job? Or maybe instead as a text file on fileserver that are mounted and also contains final results (this way we get a better documentation of how analyses were run together with result)
* fix the job spec yaml, the command and mount paths (root vs user etc)
* make sure the worker container image exists and works

## TODO cluster
* OK * Generate secret with db login

## Setup cluster for cpp pipeline
* OK * Create namespace cppipeline in cluster
* OK * Create mikro pv/pvc in namespace cppipeline
* OK * Create rancher user in namespace cppipeline
* OK * Get rancher user .kube/config-file
* OK * Generate kube config secret:
* OK   * `kubectl delete secret -n cppipeline cppipipeline-kube-conf`
* OK   * `kubectl create secret generic cppipipeline-kube-conf -n cppipeline --from-file=kube-config`
