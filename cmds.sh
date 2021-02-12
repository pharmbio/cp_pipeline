

# create cpp project in rancher gui
# add namespace to yaml file
kubectl apply -f /deployments/kubernetes/pharmbio-k8s-prod/k8s-yamls/cluster-setup/namespace-quotas-limits.yml

# add pv and pvc to yaml and apply
kubectl apply -f /deployments/kubernetes/pharmbio-k8s-prod/k8s-yamls/k8s-deployments/persistent-volumes/pharmbio-microscope-pv-pvc.yaml

# create a user in rancher gui
# download kube config from rancher gui and put in kube_config/config
kubectl create secret generic cpp-user-kube-config -n cpp --from-file kube_config/ 
kubectl create secret generic cpp-user-kube-config -n cpp-debug --from-file kube_config/

# this is for pipeline-gui
kubectl create secret generic cpp-user-kube-config -n services --from-file kube_config/

# create postgres password secret
kubectl create secret generic postgres-password -n cpp --from-file password.postgres 











