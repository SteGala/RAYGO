# Kubernetes Job Profiler
The main goal of this project is to infer the requirements (in term of RAM and CPU), and the communication pattern of a given micro-service, based on his previous executions.

The output of the system is a set of concise information regarding the previously listed properties. In particular the output is provided in the form of CRDs:
 - one `connectionprofiles.webapp.liqo.io.profiling` for each connection between microservices, describing the bandwidth requirement between them
 - one `cpuprofiles.webapp.liqo.io.profiling` for each micro-service describing the CPU requirements of that specific component
 - one `memoryprofiles.webapp.liqo.io.profiling` for each micro-service describing the Memory requirements of that specific component
 
## How to use it

Requirement | note 
--- | --- 
kustomize | Avalilable at https://kubectl.docs.kubernetes.io/installation/kustomize/ . Needed only for the first installation
kubectl | Must be correctly configured with the kubeconfig of the target cluster
prometheus | Must be installed in the cluster. The profiling system default configuration works with the standard kube-prometheus installation provided in their quick start guide https://github.com/prometheus-operator/kube-prometheus
linkerd | Must be installed in the cluster. The profiling system leverage on the metrics exported by the linkerd sidecar to generate the `connectionprofiles` CR. If Linkerd is not installed the system will work but it will not generate any `connectionprofiles` CR, it will only generate the other two previously listed

### 1) Setup
First the CRDs need to be defined in the cluster; this can be done using the command in the root directory

```shell
$ make install
```

After the CRDs have been defined, privileges (ClusterRoles and ClusterRoleBindings) need to be applied in the cluster; this can be done with the command 

```shell
$ kubectl apply -f ./config/rbac/
``` 

### 2) Configuration
After the setup phase the profiler can be instantiated in the cluster. Additional configuration parameters can be provided to customize the behaviour: they can be updated in the deployment template available under `./config/k8s/JobProfiler-deployment.yaml`, in particular the most relevant are:

Requirement | note 
--- | --- 
PROMETHEUS_URL | -
PROMETHEUS_PORT | -
PROFILING_NAMESPACE | 
BACKGROUND_ROUTINE_UPDATE_TIME | 
BACKGROUND_ROUTINE_ENABLED | 
OPERATING_MODE | 

After the configuration phase the profiler can be deployed in the cluster using the command 

```shell
$ kubectl apply -f ./config/k8s/JobProfiler-deployment.yaml
``` 

### Known limitations
- the system has been designed to work only with Deployments
- even though theoretically the profiler should be able to handle multi-container Pods, this feature has never been tested properly hence it could lead to unexpected behaviour