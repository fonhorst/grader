import datetime
import logging
import time
from typing import cast, Optional, List, Dict, Any

from kubernetes import client, config
from kubernetes.client import ApiException, V1Pod, \
    V1PodStatus, V1ContainerStateTerminated
from kubernetes.utils import parse_quantity

from grader.db.tasks import TaskStatus
from grader.env import DEFAULT_WORKER_CONFIG_PATH
from grader.tasks.base import LABEL_TASK_ID, \
    TaskContainerFailed, TaskContainerExecutionTimeout

WORKER_CONFIG_CONFIG_MAP_KEY = 'worker_config.yaml'
GRADER = 'rnseism'
K8S_ACCESS_FILE_LOCK = "k8s_access_file_lock.txt.lock"
LABEL_K8S_OWNER= 'owner'
K8S_BASE_LABEL = f'{LABEL_K8S_OWNER}={GRADER}'


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


class KubernetesManager:
    def __init__(self,
                 namespace: str = "rnseism",
                 scratch_size_bytes: int = _TEN_GYGABYTES,
                 k8s_filelock_timeout: int = 5,
                 k8s_operation_timeout: int = 5,
                 k8s_default_config_map: Optional[str] = None):
        config.load_kube_config()
        self._client = client.CoreV1Api()
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
        key, value = K8S_BASE_LABEL.split('=')
        return {key: value}

    def check_if_config_map_exists(self, config_map_name: str) -> bool:
        try:
            config_map = self.client.read_namespaced_config_map(name=config_map_name, namespace=self.namespace)
        except ApiException as ex:
            if ex.status == 404:
                return False
            raise ex

        return True

    def launch_configured_pod(self, *,
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
            raise ex

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

        while True:
            pod = cast(
                V1Pod,
                self.client.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            )

            status = cast(V1PodStatus, pod.status)

            curr_task_id: str \
                = pod.metadata.labels[LABEL_TASK_ID] if LABEL_TASK_ID in pod.metadata.labels else 'UNKNOWN'

            logger.debug("Current status of pod %s for task %s is %s" % (pod_name, curr_task_id, status.phase))

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
