import datetime
import logging
import time
import uuid
from typing import cast, Optional, List, Dict, Any

import filelock
from kubernetes import client, config
from kubernetes.client import V1NodeList, V1Node, V1PersistentVolume, V1PersistentVolumeClaim, ApiException, V1Pod, \
    V1PodStatus, V1ContainerStateTerminated
from kubernetes.utils import parse_quantity

from rnseism_sdk.db.tasks import TaskStatus
from rnseism_sdk.envs import DEFAULT_WORKER_CONFIG_PATH
from geowsm.tasks.base import LABEL_RN_PROJECT_ID, LABEL_RN_STORAGE_ID, LABEL_RN_ID, LABEL_RN_TASK_ID, \
    TaskContainerFailed, TaskContainerExecutionTimeout, NodeType
from geowsm.tasks.interactive_tasks import InteractiveWorkerStatus
from geowsm.tasks.nodes import Node, Volume, NetworkStorage, \
    NetworkStorageVolume, VolumeSizeException, UnknownNetworkStorageException, NodeManagementException, \
    KubernetesException, NoSuchVolumeException, VolumeAlreadyExistsException

WORKER_CONFIG_CONFIG_MAP_KEY = 'worker_config.yaml'

RNSEISM = 'rnseism'

K8S_ACCESS_FILE_LOCK = "k8s_access_file_lock.txt.lock"

# Kube Nodes Labels
LABEL_K8S_RN_OWNER= 'owner'
LABEL_K8S_RN_NODE_TYPE_LABEL_KEY = 'rn_node_type'
LABEL_K8S_RN_PRIORITY_PROJECT_ID = 'rn_priority_project_id'


K8S_RNSEISM_BASE_LABEL = f'{LABEL_K8S_RN_OWNER}={RNSEISM}'
K8S_RNSEISM_INTERACTIVE_NODE_LABEL = f'{LABEL_K8S_RN_NODE_TYPE_LABEL_KEY}={NodeType.interactive.value}'
K8S_RNSEISM_COMPUTE_NODE_LABEL = f'{LABEL_K8S_RN_NODE_TYPE_LABEL_KEY}={NodeType.compute.value}'


logger = logging.getLogger(__name__)

_TEN_GYGABYTES = 10 * 1024 * 1024 * 1024

def size2bytes(size: str) -> int:
    return int(parse_quantity(size))


def extract_exit_code(pod: V1Pod) -> int:
    name = pod.metadata.name
    state = next(cstatus for cstatus in pod.status.container_statuses if cstatus.name == name).state
    assert state.terminated is not None
    state_terminated = cast(V1ContainerStateTerminated, state.terminated)
    return state_terminated.exit_code


def k8s_status_to_task_status(status: str) -> TaskStatus:
    if status == 'Pending':
        return TaskStatus.RUNNING
    if status == 'Running':
        return TaskStatus.RUNNING
    if status == 'Succeeded':
        return TaskStatus.FINISHED
    if status == 'Failed':
        return TaskStatus.FAILED

    raise ValueError(f"Unsupported pod status {status}. Cannot convert it to TaskStatus")


def k8s_status_to_iworker_status(status: str) -> InteractiveWorkerStatus:
    if status == 'Pending':
        return InteractiveWorkerStatus.running
    if status == 'Running':
        return InteractiveWorkerStatus.running
    if status == 'Succeeded':
        return InteractiveWorkerStatus.stopped
    if status == 'Failed':
        return InteractiveWorkerStatus.failed

    raise ValueError(f"Unsupported pod status {status}. Cannot convert it to InteractiveWorkerStatus")


class KubernetesManager:
    def __init__(self,
                 network_storages: List[NetworkStorage],
                 namespace: str = "rnseism",
                 scratch_size_bytes: int = _TEN_GYGABYTES,
                 k8s_filelock_timeout: int = 5,
                 k8s_operation_timeout: int = 5,
                 k8s_default_config_map: Optional[str] = None):
        config.load_kube_config()
        self._client = client.CoreV1Api()
        self._network_storages: Dict[str, NetworkStorage] = {storage.uid: storage for storage in network_storages}
        self._namespace = namespace
        self._scratch_size_bytes = scratch_size_bytes
        self._k8s_filelock_timeout = k8s_filelock_timeout
        self._k8s_operation_timeout = k8s_operation_timeout
        self._k8s_default_config_map = k8s_default_config_map

    @property
    def client(self) -> client.CoreV1Api:
        return self._client

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def scratch_size_bytes(self) -> int:
        return self._scratch_size_bytes

    @property
    def k8s_operation_timeout(self) -> int:
        return self._k8s_operation_timeout

    @staticmethod
    def base_labels() -> Dict[str, str]:
        key, value = K8S_RNSEISM_BASE_LABEL.split('=')
        return {key: value}

    def validate_volumes_sizes(self):
        volumes = self.list_storage_volumes()
        storage_uids = {volume.storage_uid for volume in volumes}
        for storage_uid in storage_uids:
            allocated_space_size = self._calculate_allocated_space_size(storage_uid)
            storage = self._network_storages[storage_uid]
            if allocated_space_size > storage.size_bytes:
                raise VolumeSizeException(f"Overall allocated space size ({allocated_space_size}) "
                                          f"is greater than maximum size ({storage.size_bytes}) "
                                          f"of storage {storage_uid}")

    def check_if_config_map_exists(self, config_map_name: str) -> bool:
        try:
            config_map = self.client.read_namespaced_config_map(name=config_map_name, namespace=self.namespace)
        except ApiException as ex:
            if ex.status == 404:
                return False
            raise KubernetesException() from ex

        return True

    def launch_configured_pod(self, *,
                              project_id: str,
                              pod_name: str,
                              image: str,
                              hostname: Optional[str] = None,
                              labels: Optional[Dict[str, str]] = None,
                              node_selector: Optional[Dict[str, str]] = None,
                              command: Optional[str] = None,
                              args: Optional[List[str]] = None,
                              env: Optional[Dict[str, str]] = None,
                              cpu: Optional[str] = None,
                              memory: Optional[str] = None,
                              termination_graceful_timeout: int = 5,
                              volumes: Optional[List[Dict[str, Any]]] = None,
                              service_ports: Optional[List[int]] = None,
                              service_type: str = 'ClusterIP', # 'NodePort'
                              service_account_name: Optional[str] = None,
                              worker_config_path: Optional[str] = None,
                              worker_config_configmap: Optional[str] = None) -> V1Pod:
        logger.info("Launching pod %s" % pod_name)

        if worker_config_path and worker_config_configmap:
            raise ValueError("Either worker_config_path or worker_config_configmap should be supplied. "
                             "Not both of them.")

        if not worker_config_path and not worker_config_configmap and not self._k8s_default_config_map:
            raise ValueError("Either worker_config_path or worker_config_configmap should be supplied. "
                             "Neither provided and default config map is not set in the kubernetes manager")

        args_and_command = {
            k: v
            for k, v in {'command': command, 'args': args}.items()
            if v is not None
        }

        env_vars = [{'name': k, 'value': v} for k, v in env.items()] if env else []

        resources = {
            k: v
            for k, v in {'cpu': cpu, 'memory': memory}.items()
            if v is not None
        }

        if len(resources) > 0:
            resources = {
                "requests": resources,
                "limits": resources
            }

        config_map_body = None
        if worker_config_path:
            with open(worker_config_path, 'r') as f:
                config_data = f.read()

            config_map_body = {
                'metadata': {
                    'name': pod_name,
                    'labels': labels
                },
                'data': {
                    WORKER_CONFIG_CONFIG_MAP_KEY: config_data
                }
            }
            config_map_name = pod_name
        elif worker_config_configmap:
            config_map_name = worker_config_configmap
        else:
            config_map_name = self._k8s_default_config_map

        storage_volumes = self.list_storage_volumes(project_uid=project_id)

        if service_ports:
            svc_name = f'{pod_name}-svc'
            headless_svc_name = f'{pod_name}'

            svc_body = {
                'metadata': {
                    'name': svc_name,
                    'labels': labels,
                    'ownerReference': pod_name
                },
                'spec': {
                    'type': service_type,
                    'ports': [
                        {
                            'port': sport,
                            'protocol': 'TCP',
                            'name': f'tcp-{sport}'
                        }
                        for sport in service_ports
                    ],
                    'selector': {
                        'app': pod_name
                    }
                }
            }

            headless_svc_body = {
                "metadata": {
                    'name': headless_svc_name,
                    'labels': labels,
                    'ownerReference': pod_name
                },
                'spec': {
                    'type': 'ClusterIP',
                    'clusterIP': None,
                    'ports': [
                        {
                            'port': sport,
                            'protocol': 'TCP',
                            'name': f'tcp-{sport}'
                        }
                        for sport in service_ports
                    ],
                    'selector': {
                        'app': pod_name
                    }
                }
            }

            pod_labels = {
                'app': pod_name,
                **(labels or dict())
            }
        else:
            svc_name = None
            headless_svc_name = None
            svc_body = None
            headless_svc_body = None
            pod_labels = labels

        pod_body = {
            'metadata': {
                'name': pod_name,
                'labels': pod_labels
            },
            'spec': {
                'containers': [
                    {
                        'name': pod_name,
                        'image': image,
                        'imagePullPolicy': 'Always',
                        'env': env_vars,
                        'volumeMounts': [
                            {
                                'name': 'worker-config',
                                'mountPath': DEFAULT_WORKER_CONFIG_PATH,
                                'subPath': WORKER_CONFIG_CONFIG_MAP_KEY,
                                'readOnly': True
                            },
                            *(
                                {
                                    'name': volume.uid,
                                    'mountPath': f"/mnt/net_storage_{volume.storage_uid}",
                                }
                                for volume in storage_volumes
                            ),
                            *(
                                [
                                    volume['volumeMount']
                                    for volume in volumes
                                ]
                                if volumes else []
                            )
                        ],
                        'resources': resources,
                        **args_and_command
                    }
                ],
                'hostname': hostname or pod_name,
                'nodeSelector': node_selector,
                'restartPolicy': 'Never',
                'terminationGracePeriodSeconds': termination_graceful_timeout,
                **({'serviceAccountName': service_account_name} if service_account_name else dict()),
                'volumes': [
                    {
                        'name': 'worker-config',
                        'configMap': {
                            'name': config_map_name,
                            'items': [{'key': WORKER_CONFIG_CONFIG_MAP_KEY, 'path': WORKER_CONFIG_CONFIG_MAP_KEY}]
                        }
                    },
                    *(
                        {
                            'name': volume.uid,
                            'persistentVolumeClaim': {
                                'claimName': volume.uid
                            }
                        }
                        for volume in storage_volumes
                    ),
                    *(
                        [
                            volume['volume']
                            for volume in volumes
                        ]
                        if volumes else []
                    )
                ]
            }
        }

        logger.debug(
            "Using the following configuration for pod %s "
            "to launch it in namespace %s. "
            "ConfigMap: %s. Pod: %s." % (pod_name, self.namespace, config_map_body, pod_body)
        )

        try:
            if config_map_body:
                logger.debug("Creating config map %s for pod %s" % (pod_name, pod_name))
                self.client.create_namespaced_config_map(namespace=self.namespace, body=config_map_body)

            logger.debug("Creating pod %s" % pod_name)
            pod = cast(V1Pod, self.client.create_namespaced_pod(namespace=self.namespace, body=pod_body))

            if svc_body:
                logger.debug("Creating headless svc %s" % headless_svc_name)
                self.client.create_namespaced_service(namespace=self.namespace, body=headless_svc_body)

                logger.debug("Creating svc %s" % svc_name)
                self.client.create_namespaced_service(namespace=self.namespace, body=svc_body)
        except ApiException as ex:
            logger.error("Kubernetes ApiException error", exc_info=True)
            raise KubernetesException() from ex

        logger.info("Pod %s has been successfully launched" % pod_name)

        return pod

    def wait_for_pod(self,
                     pod_name: str,
                     status_check_time_interval: int = 1,
                     timeout_to_get_running: int = 10,
                     timeout: Optional[int] = None) -> int:
        # https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#podstatus-v1-core
        logger.info("Waiting for pod %s" % pod_name)

        begin = datetime.datetime.now()
        is_running = False

        # TODO: can be replaced with Watch. Consider it later.
        while True:
            try:
                pod = cast(
                    V1Pod,
                    self.client.read_namespaced_pod(name=pod_name, namespace=self.namespace)
                )
            except ApiException as ex:
                raise KubernetesException() from ex

            status = cast(V1PodStatus, pod.status)

            curr_task_id: str \
                = pod.metadata.labels[LABEL_RN_TASK_ID] if LABEL_RN_TASK_ID in pod.metadata.labels else 'UNKNOWN'

            logger.debug("Current status of pod %s for task %s is %s" % (pod_name, curr_task_id, status.phase))

            # TODO: process 'Evicted' state
            # TODO: process external pod deletion

            if status.phase == 'Running':
                is_running = True

            if status.phase == 'Failed':
                logger.warning("Pod of task %s failed for some reason: %s " % (status.phase, status.message))
                exit_code = extract_exit_code(pod)

                raise TaskContainerFailed(f"Container {pod_name} of task {curr_task_id} "
                                          f"failed with exit code {exit_code}")

            if status.phase == 'Succeeded':
                exit_code = extract_exit_code(pod)

                logger.info("Task %s successfully finished with exit_code %s"
                            % (curr_task_id, exit_code))

                return exit_code

            elapsed_time = (datetime.datetime.now() - begin).total_seconds()
            # TODO: handle container creating
            if not is_running and elapsed_time > timeout_to_get_running:
                raise TaskContainerFailed(f"Container {pod_name} of task {curr_task_id} "
                                          f"has not transfered to 'Running' status "
                                          f"before timeout {timeout_to_get_running}. "
                                          f"Elapsed seconds: {elapsed_time}")

            if timeout is not None and elapsed_time > timeout:
                raise TaskContainerExecutionTimeout(f"Container {pod_name} of task {curr_task_id} "
                                                    f"has not finished before timeout {timeout}. "
                                                    f"Elapsed seconds: {elapsed_time}.")

            time.sleep(status_check_time_interval)

    def list_nodes(self, node_type: Optional[NodeType] = None) -> List[Node]:
        if not node_type:
            label_selector = [K8S_RNSEISM_BASE_LABEL]
        elif node_type == NodeType.interactive:
            label_selector = [K8S_RNSEISM_BASE_LABEL, K8S_RNSEISM_INTERACTIVE_NODE_LABEL]
        else:
            label_selector = [K8S_RNSEISM_BASE_LABEL, K8S_RNSEISM_COMPUTE_NODE_LABEL]

        label_selector = ','.join(label_selector)

        try:
            kube_nodes = cast(V1NodeList, self._client.list_node(label_selector=label_selector))
        except ApiException as ex:
            raise KubernetesException() from ex

        nodes = [self._from_k8s_node(node) for node in kube_nodes.items]
        return nodes

    def get_node(self, node_uid: str) -> Optional[Node]:
        try:
            node = self._client.read_node(name=node_uid)
        except ApiException as ex:
            if ex.status != 404:
                raise KubernetesException() from ex
            return None

        if node.metadata.labels.get(LABEL_K8S_RN_OWNER, None) != RNSEISM:
            return None

        return self._from_k8s_node(node)

    def set_node_priority_project(self, node_uid: str, project_uid: str, force: bool = False) -> Node:
        return self._update_priority_project(node_uid, project_uid=project_uid, force=force)

    def unset_node_priority_project(self, node_uid: str) -> Node:
        return self._update_priority_project(node_uid, project_uid=None)

    def list_storages(self, project_uid: Optional[str] = None) -> List[NetworkStorageVolume]:
        volumes = self.list_storage_volumes(project_uid)

        if len(volumes) == 0 and project_uid:
            volumes = [
                Volume(uid=None, storage_uid=nst.uid, project_uid=project_uid, size_bytes=0)
                for nst in self._network_storages.values()
            ]

        storage_uids = {volume.storage_uid for volume in volumes}
        storage_allocated_sizes = {
            storage_uid: self._calculate_allocated_space_size(storage_uid)
            for storage_uid in storage_uids
        }
        return [
            NetworkStorageVolume(
                uid=volume.uid,
                storage_uid=volume.storage_uid,
                project_uid=volume.project_uid,
                volume_size_bytes=volume.size_bytes,
                storage_size_bytes=self._network_storages[volume.storage_uid].size_bytes,
                allocated_size_bytes=storage_allocated_sizes[volume.storage_uid],
                network_storage_base_host_path=self._network_storages[volume.storage_uid].network_storage_base_host_path
            )
            for volume in volumes
        ]

    def list_storage_volumes(self, project_uid: Optional[str] = None) -> List[Volume]:
        if project_uid:
            label_selector = [K8S_RNSEISM_BASE_LABEL, f'{LABEL_RN_PROJECT_ID}={project_uid}']
        else:
            label_selector = [K8S_RNSEISM_BASE_LABEL]

        label_selector = ','.join(label_selector)

        try:
            volume_claims = self._client.list_namespaced_persistent_volume_claim(
                namespace=self._namespace,
                label_selector=label_selector
            ).items
        except ApiException as ex:
            raise KubernetesException() from ex

        return [self._from_k8s_volume_claim(v) for v in volume_claims]

    def create_storage_volume(self, storage_uid: str, project_uid: str, size_bytes: int) -> Volume:
        with self._lock():
            return self._create_storage_volume(storage_uid, project_uid, size_bytes)

    def delete_storage_volume(self, volume_uid: str, allow_missing: bool = False):
        with self._lock():
            self._delete_storage_volume(volume_uid, allow_missing)

    def update_storage_volume(self, volume_uid: str, size_bytes: int) -> Volume:
        logger.info("Updating volume %s to size %s" % (volume_uid, size_bytes))

        with self._lock():
            logger.debug("Reading current PVC info for volume %s" % volume_uid)

            try:
                volume_claim = self._client.read_namespaced_persistent_volume_claim(
                    namespace=self._namespace,
                    name=volume_uid
                )
            except ApiException as ex:
                raise KubernetesException() from ex

            volume = self._from_k8s_volume_claim(volume_claim)
            storage = self._get_storage(volume.storage_uid)

            # check size of new volume is bigger than the existing one (we cannot reduce size of the volume)
            if volume.size_bytes >= size_bytes:
                raise VolumeSizeException(f"Cannot update volume {volume_uid} because "
                                          f"its current size {volume.size_bytes} is greater "
                                          f"or equal to new size {size_bytes}")

            # calculate if we are able to increase size
            self._check_if_possible_to_allocate(storage, size_bytes - volume.size_bytes)

            # delete the old volume
            self._delete_storage_volume(volume_uid)

            # sometimes the entity is stil presented in list of pvc possibly due to ongoing termination
            time.sleep(1)

            # create the new one
            volume = self._create_storage_volume(
                storage_uid=volume.storage_uid,
                project_uid=volume.project_uid,
                size_bytes=size_bytes
            )

        logger.info("Successfully updated volume %s to size %s" % (volume_uid, size_bytes))

        return volume

    def _create_storage_volume(self, storage_uid: str, project_uid: str, size_bytes: int) -> Volume:
        logging.info(
            "Creating volume of %s bytes for project %s in storage %s" % (size_bytes, project_uid, storage_uid)
        )

        storage = self._get_storage(storage_uid)

        self._check_if_possible_to_allocate(storage, size_bytes)

        uid = str(uuid.uuid4())
        volume_name = f'net-storage-{storage_uid}-{project_uid}'

        logger.debug(
            "Creating persistent volume %s for project %s in storage %s" % (volume_name, project_uid, storage_uid)
        )

        body = {
            'metadata':
                {
                    'name': volume_name,
                    'labels': {
                        **self.base_labels(),
                        LABEL_RN_STORAGE_ID: storage.uid,
                        LABEL_RN_PROJECT_ID: project_uid,
                        LABEL_RN_ID: uid
                    }
                },
            'spec': {
                'capacity':{
                    'storage': size_bytes
                },
                'volumeMode': 'Filesystem',
                'accessModes': ['ReadWriteMany'],
                'persistentVolumeReclaimPolicy': 'Retain',
                'storageClassName': storage.uid,
                'hostPath': {
                    'path': storage.network_storage_base_host_path,
                    'type': 'DirectoryOrCreate'
                }
            }
        }
        try:
            cast(V1PersistentVolume, self._client.create_persistent_volume(body=body))
        except ApiException as ex:
            if ex.status == 409:
                raise VolumeAlreadyExistsException() from ex

            raise KubernetesException() from ex

        logger.debug(
            "Creating persistent volume claim %s for project %s in storage %s" % (volume_name, project_uid, storage_uid)
        )

        body = {
            'metadata':
                {
                    'name': volume_name,
                    'labels': {
                        **self.base_labels(),
                        LABEL_RN_STORAGE_ID: storage.uid,
                        LABEL_RN_PROJECT_ID: project_uid,
                        LABEL_RN_ID: uid
                    }
                },
            'spec': {
                'storageClassName': storage.uid,
                'accessModes': ['ReadWriteMany'],
                'volumeName': volume_name,
                'resources': {
                    'requests': {
                        'storage': size_bytes
                    }
                }
            }
        }

        try:
            volume_claim = cast(
                V1PersistentVolumeClaim,
                self._client.create_namespaced_persistent_volume_claim(namespace=self._namespace, body=body)
            )
        except ApiException as ex:
            raise KubernetesException() from ex

        logger.info(
            "Successfully created volume %s for project %s in storage %s" % (volume_name, project_uid, storage_uid)
        )

        return self._from_k8s_volume_claim(volume_claim)

    def _delete_storage_volume(self, volume_uid: str, allow_missing: bool = False):
        logger.info(
            "Deleting volume %s (allow_missing %s)" % (volume_uid, allow_missing)
        )

        try:
            logger.debug("Deleting persistent volume claim %s" % volume_uid)
            self._client.delete_namespaced_persistent_volume_claim(
                name=volume_uid,
                namespace=self._namespace,
                grace_period_seconds=self._k8s_operation_timeout
            )
        except ApiException as ex:
            if ex.status == 404 and allow_missing:
                logger.warning("Persistent volume claim %s not found for deleting. Skip it and continue." % volume_uid)
                pass
            else:
                raise NoSuchVolumeException(f"Can not delete volume {volume_uid}. "
                                            f"PVC or/and PV with name {volume_uid} not found.") from ex
        try:
            self._client.delete_persistent_volume(
                name=volume_uid,
                grace_period_seconds=self._k8s_operation_timeout
            )
        except ApiException as ex:
            if ex.status == 404 and allow_missing:
                logger.warning("Persistent volume %s not found for deleting. Skip it and continue." % volume_uid)
            else:
                raise KubernetesException(f"Can not delete volume {volume_uid}. "
                                          f"PVC or/and PV with name {volume_uid} not found.") from ex

        logger.info("Succesfully deleted volume %s" % volume_uid)

    def _lock(self) -> filelock.FileLock:
        return filelock.FileLock(K8S_ACCESS_FILE_LOCK, timeout=self._k8s_filelock_timeout)

    def _update_priority_project(self, node_uid: str, project_uid: Optional[str], force: bool = False) -> Node:
        logger.info("Setting node %s as prioritized for project %s (force=%s)" % (node_uid, project_uid, force))

        with self._lock():
            logger.debug("Reading node %s labels" % node_uid)

            try:
                kube_node = cast(V1Node, self._client.read_node(name=node_uid))
            except ApiException as ex:
                raise KubernetesException() from ex

            labels: Dict[str, str] = kube_node.metadata.labels

            if project_uid is not None and LABEL_K8S_RN_PRIORITY_PROJECT_ID in labels and not force:
                action = 'set priority project ' + str(project_uid) if project_uid else 'unset priority project'
                raise NodeManagementException(f"Cannot {action} for node {node_uid} "
                                              f"due to the node already has priority "
                                              f"project {labels[LABEL_K8S_RN_PRIORITY_PROJECT_ID]} and force={force}")

            logger.debug("Patching node %s labels" % node_uid)

            body = {
                "metadata": {
                    "labels": {
                        LABEL_K8S_RN_PRIORITY_PROJECT_ID: project_uid
                    }
                }
            }
            try:
                kube_node = cast(V1Node, self._client.patch_node(name=node_uid, body=body))
            except ApiException as ex:
                raise KubernetesException() from ex

        logger.info("Succesfully set node %s as prioritized for project %s (force=%s)" % (node_uid, project_uid, force))

        return self._from_k8s_node(kube_node)

    def _get_storage(self, storage_uid) -> NetworkStorage:
        if storage_uid not in self._network_storages:
            raise UnknownNetworkStorageException(f"Storage {storage_uid} is not supported. "
                                        f"Available storages: {self._network_storages.keys()}")

        storage = self._network_storages[storage_uid]
        return storage

    def _check_if_possible_to_allocate(self, storage: NetworkStorage, size_bytes: int):
        allocated_size = self._calculate_allocated_space_size(storage.uid)
        free_size = storage.size_bytes - allocated_size

        logger.debug("Found allocated size %s for storage %s (asking to allocate %s bytes)"
                     % (allocated_size, storage.uid, size_bytes))

        if free_size < size_bytes:
            raise VolumeSizeException(f"Not enough free space to allocate a new volume: "
                                      f"available - {free_size}, requested - {size_bytes}")

    def _calculate_allocated_space_size(self, storage_uid: str) -> int:
        label_selector = ','.join([f'{LABEL_RN_STORAGE_ID}={storage_uid}'])
        try:
            claims= self._client.list_namespaced_persistent_volume_claim(
                namespace=self._namespace,
                label_selector=label_selector
            ).items
        except ApiException as ex:
            raise KubernetesException() from ex

        allocated_size = sum(size2bytes(claim.spec.resources.requests['storage']) for claim in claims)
        return allocated_size

    def _from_k8s_node(self, node: V1Node) -> Node:
        addrs: Dict[str, str] = {addr.type: addr.address for addr in node.status.addresses}
        name: str = node.metadata.name
        labels: Dict[str, str] = node.metadata.labels
        allocatable: Dict[str, str] = node.status.allocatable
        rn_priority_project_id = labels.get(LABEL_K8S_RN_PRIORITY_PROJECT_ID, None)
        return Node(
            uid=name,
            name=addrs['Hostname'],
            ip=addrs['InternalIP'],
            node_type=NodeType(labels[LABEL_K8S_RN_NODE_TYPE_LABEL_KEY]),
            priority_project_id=rn_priority_project_id,
            scratch_size_bytes=self._scratch_size_bytes,
            hdd_size_bytes=allocatable['ephemeral-storage']
        )

    @staticmethod
    def _from_k8s_volume_claim(volume_claim: V1PersistentVolumeClaim) -> Volume:
        name = volume_claim.metadata.name
        labels: Dict[str, str] = volume_claim.metadata.labels
        storage_uid = labels[LABEL_RN_STORAGE_ID]
        project_uid = labels[LABEL_RN_PROJECT_ID]
        size_bytes = size2bytes(volume_claim.spec.resources.requests['storage'])
        return Volume(
            uid=name,
            storage_uid=storage_uid,
            project_uid=project_uid,
            size_bytes=size_bytes
        )
