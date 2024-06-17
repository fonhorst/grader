To run geowsm with k8s for task execution only do the following:
1. For geowsm service set the following env vars:
```commandline
WMS_EXECUTION_BACKEND=kubernetes
WMS_RUNNER_DB_CONN_EXTERNAL=10.64.24.6:5432
WMS_CELERY_BROKER_URL_EXTERNAL=10.64.24.6:5672
ENV_WMS_CELERY_RESULT_BACKEND_EXTERNAL=10.64.24.6:6379
```

Mount k8s config file into geowsm service container:
```commandline
/home/nikolay/.kube/config:/root/.kube/config
```

2. For batch_worker service set the following env vars:
```commandline
WMS_BATCH_WORKER_TYPE=kubernetes
WMS_RUNNER_DB_CONN_EXTERNAL=10.64.24.6:5432
```

Mount k8s config file into batch_worker service container:
```commandline
/home/nikolay/.kube/config:/root/.kube/config
```

3. Create a special config map with correct config for batch tasks in the namespace for pods to run
```commandline
apiVersion: v1
kind: ConfigMap
metadata:
  labels:
    owner: rnseism
  name: external-configmap
  namespace: rnseism-test
data:
  worker_config.yaml: |
    jobs: {}
    jobs_path: /jobs
    kubernetes_manager:
      k8s_default_config_map: external-configmap
      k8s_filelock_timeout: 5
      k8s_operation_timeout: 5
      namespace: rnseism-test
      network_storages:
      - network_storage_base_host_path: /mnt/ess_storage/DN_1/tmp/test_rnseism_storages/storage_a
        size_bytes: 10737418240
        uid: astorage
      - network_storage_base_host_path: /mnt/ess_storage/DN_1/tmp/test_rnseism_storages/storage_b
        size_bytes: 21474836480
        uid: bstorage
      scratch_size_bytes: 10737418240
    parameters_manager: redis://10.64.24.6:6379/5
    runner:
      db_url: postgresql://postgres:postgres@10.64.24.6:5432/wms
      result_storage_url: redis://10.64.24.6:6379/2
      session_status_storage_url: redis://10.64.24.6:6379/4
      state_storage_url: redis://10.64.24.6:6379/3
      storage_url: 10.32.0.243:8095
```

The important parts:
* 'runner' section should contain urls of required databases with ips and ports available for pods to access outside of the cluster 
* 'kubernetes_manager.k8s_default_config_map' should contain the name of configmap being created
