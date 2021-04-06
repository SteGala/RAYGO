# Kubernetes Job Profiler
The main goal of this project is to infer the requirements (in term of RAM and CPU), and the communication pattern of a given micro-service, based on his previous executions.

The output of the system is a set of concise information regarding the previously listed properties. In particular the output is provided in the form of CRDs:
 - one `connectionprofiles.webapp.liqo.io.profiling` for each connection between microservices, describing the bandwidth requirement between them
 - one `cpuprofiles.webapp.liqo.io.profiling` for each micro-service describing the CPU requirements of that specific component
 - one `memoryprofiles.webapp.liqo.io.profiling` for each micro-service describing the Memory requirements of that specific component

 Each Pod will have a set of labels to referenciate those resources. If we get for example the Pod yaml we will see:

```yaml
...
metadata:
  annotations:
    ...
    liqo.io/connectionProfile0: connprofile-f6fcbfcb54911a4ae903300e39472d9af57f5e9f
    liqo.io/connectionProfile1: connprofile-fc09272092ec5b7073153c9134d0156117451784
    liqo.io/connectionProfile2: connprofile-b2d1d9faa4282ddf04a37d45d30192f1d6bbdb33
    liqo.io/cpuProfile: cpuprofile-9b9e16387237056ac52a193ffbd6d695831cfe7f
    liqo.io/memoryProfile: memprofile-9b9e16387237056ac52a193ffbd6d695831cfe7f
    ...

```

Where memoryprofile has the content:
```yaml
...
spec:
  memoryProfiling:
    updateTime: 2021-04-06 06:53:01.64479791 +0000 UTC m=+15.292131024
    value: "17106488" # expressed in bytes

```

cpuprofile has a similar structure:
```yaml
...
spec:
  cpuProfiling:
    updateTime: 2021-04-06 06:53:01.64479791 +0000 UTC m=+15.292131024
    value: "0.020" # expressed in number of cores

```

and connectionprofile:
```yaml
...
spec:
  bandwidth_requirement: "47.12"
  destination_job: voting
  destination_namespace: emojivoto
  source_job: web
  source_namespace: emojivoto
  update_time: 2021-04-06 06:53:01.64479791 +0000 UTC m=+15.292131024

```
describing both source and destination jobs and the bandwidth requirement between them expressed in term of average number of (requests+responses) per minute.

## How to use it

Requirement | note 
--- | --- 
golang | -
kustomize | Avalilable at https://kubectl.docs.kubernetes.io/installation/kustomize/ . Needed only for the first installation
kubectl | Must be correctly configured with the kubeconfig of the target cluster
linkerd | Must be installed in the cluster. The profiling system leverage on the metrics exported by the linkerd sidecar to generate the `connectionprofiles` CR. If Linkerd is not installed the system will work but it will not generate any `connectionprofiles` CR, it will only generate the other two previously listed
prometheus | Must be installed in the cluster. The profiling system default configuration works with the standard kube-prometheus installation provided in the installation guide in linkerd quick-start guide

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
After the setup phase the profiler can be instantiated in the cluster. Additional configuration parameters can be provided to customize the behaviour: they can be updated in the pod template available under `./config/k8s/JobProfiler-pod.yaml`, in particular the most relevant are:

Requirement | note 
--- | --- 
PROMETHEUS_URL | -
PROMETHEUS_PORT | -
PROFILING_NAMESPACE | the namespace name of the application we would like to profile
BACKGROUND_ROUTINE_UPDATE_TIME | the frequency of update of the output CRs

My suggestion is to keep the other setup parameters as the are because they do not affect the execution, their only purpose is to tune the output values. After the configuration phase the profiler can be deployed in the cluster using the command 

```shell
$ kubectl apply -f ./config/k8s/JobProfiler-pod.yaml
``` 

### Known limitations
- the system has been designed to work only with Deployments
- even though theoretically the profiler should be able to handle multi-container Pods, this feature has never been tested properly hence it could lead to unexpected behaviour
- if Prometheus doesn't have informations about the microservices (i.e. the application is deployed for the first time, or it hasn't been deployed for a while), the profiler will generate the output for memory and cpu, but it won't generate any connection profiling output. Given the fact that the profiler has been designed as a loop sooner or later those outputs will be generated too (but due to some code limitation no labels regarding connection profiling will be added to the pod)