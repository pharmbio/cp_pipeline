# Cellprofiler pipelines automation on Kubernetes

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








