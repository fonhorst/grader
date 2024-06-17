import enum
import logging
import os
import traceback
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Dict, Any, Optional, List, cast

import docker
from billiard.exceptions import SoftTimeLimitExceeded
from celery import shared_task
from docker.models.containers import Container
from kubernetes.client import V1Pod, V1ContainerStateTerminated, ApiException

from rnseism_sdk.db.tasks import update_task_status, TaskStatus, report_task_fail_reason, TaskType
# TODO: uncomment env imports when new rnseism_sdk referenced`
from rnseism_sdk.envs import (
    ENV_VAR_RUNNER_DB_CONN, DEFAULT_WORKER_CONFIG_PATH, ENV_VAR_TOKEN, ENV_VAR_RUNNER_DB_CONN_EXTERNAL,
    ENV_VAR_LOGGING_LEVEL, # ENV_VAR_HDFS_CLUSTER_NAME, ENV_VAR_HDFS_NAMENODE_HEAPSIZE, ENV_VAR_HDFS_NN_RPC_URI,
    # ENV_VAR_HDFS_NN_HTTP_URI, ENV_VAR_HDFS_DATANODE_HOSTNAME
)

from grader.env import ENV_VAR_BATCH_WORKER_REMOVE_CONTAINER_POLICY, \
    ENV_VAR_BATCH_WORKER_CONFIG_VOLUME, \
    ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND, ENV_VAR_TASK_ID, ENV_VAR_JOB_ID, \
    ENV_VAR_WORKER_NETWORK, ENV_VAR_BATCH_WORKER_TYPE
from rnseism_sdk.runner.base import RunnerException, Runner
from grader.tasks.base import TaskResult, LABEL_RN_ENTITY_TYPE, RNSEISM_BATCH_TASK, \
    LABEL_RN_TASK_ID, LABEL_RN_JOB_ID, LABEL_RN_PROJECT_ID, LABEL_RN_USER_ID, LABEL_RN_TASK_TYPE, LABEL_RN_ID, \
    TaskContainerFailed, NodeType
from rnseism_sdk.worker.base import ParametersManager
from grader.tasks.interactive_tasks import InteractiveTaskRunArgs
from grader.tasks.kubernetes.kubernetes_manager import KubernetesManager, \
    LABEL_K8S_RN_OWNER, RNSEISM, LABEL_K8S_RN_NODE_TYPE_LABEL_KEY
from grader.tasks.utils import try_pull_image
from grader.tasks.batch_tasks_args import BatchTaskRunArgs, SparkOnK8sBatchTaskRunArgs

logger = logging.getLogger(__name__)

WMS_JOBS_PATH = "WMS_JOBS_PATH"


class BatchWorkerType(enum.Enum):
    docker = 'docker'
    kubernetes = 'kubernetes'


_current_runner: Optional[Runner] = None
_current_parameters_manager: Optional[ParametersManager] = None
_current_kubernetes_manager: Optional[KubernetesManager] = None


def get_batch_worker_type() -> Optional[BatchWorkerType]:
    type = os.environ.get(ENV_VAR_BATCH_WORKER_TYPE, None)
    return BatchWorkerType(type) if type else None

def set_current_runner(runner: Runner):
    global _current_runner
    _current_runner = runner


def current_runner() -> Optional[Runner]:
    return _current_runner


def set_current_parameters_manager(storage: ParametersManager):
    global _current_parameters_manager
    _current_parameters_manager = storage


def current_parameters_manager() -> Optional[ParametersManager]:
    return _current_parameters_manager


def set_current_kubernetes_manager(manager: KubernetesManager):
    global _current_kubernetes_manager
    _current_kubernetes_manager = manager


def current_kubernetes_manager() -> Optional[KubernetesManager]:
    return _current_kubernetes_manager


@contextmanager
def parameters(task_uid: str, parameters: Dict[str, Any]):
    manager = current_parameters_manager()
    if not manager:
        raise ValueError("Parameters manager is not available, but required")

    manager.put(task_uid, parameters)

    yield

    manager.remove(task_uid)


class ContainerRemovePolicy(enum.Enum):
    always = 'always'
    on_success = 'on_success'
    never = 'never'

    def should_remove(self, is_failed: bool) -> bool:
        if self == self.never:
            return False

        if (self == self.on_success) and is_failed:
            return False

        return True



class BatchTaskExecutor(ABC):
    @classmethod
    def parse_args(cls, args: Dict[str, Any]) -> BatchTaskRunArgs:
        return BatchTaskRunArgs.parse_obj(args)

    def __init__(self, token: str, curr_task_id: str, args: Dict[str, Any]):
        self.token = token
        self._curr_task_id = curr_task_id
        self.run_args: BatchTaskRunArgs = self.parse_args(args)

    def run(self) -> Dict[str, Any]:
        logger.info(f"Received task with args: {self.run_args.dict()}")

        env_vars = [
            ENV_VAR_RUNNER_DB_CONN, ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND
        ]

        not_presented_env_vars = [env_var for env_var in env_vars if env_var not in os.environ]

        if len(not_presented_env_vars) > 0:
            raise ValueError(f"The following env vars are not set: {not_presented_env_vars}")

        with parameters(self._curr_task_id, self.run_args.parameters):
            failed = False
            try:
                logger.info(f"Starting task {self._curr_task_id}. Updating status to {TaskStatus.RUNNING}")
                update_task_status(task_id=self._curr_task_id, status=TaskStatus.RUNNING)

                logger.info("Launching container for task %s" % self._curr_task_id)
                self.launch_container()

                logger.info(f"Finished task {self._curr_task_id}. Updating status to {TaskStatus.FINISHED}")
                update_task_status(task_id=self._curr_task_id, status=TaskStatus.FINISHED)

                return TaskResult(task_id=self._curr_task_id, result=dict()).dict()
            except SoftTimeLimitExceeded:
                logger.warning(f"Either soft time limited happened or the task ({self._curr_task_id}) was revoked. "
                               f"Updating status to {TaskStatus.CANCELLED}", exc_info=True)
                update_task_status(task_id=self._curr_task_id, status=TaskStatus.CANCELLED)
                failed = True
                raise
            except Exception:
                logger.error(f"Unexpected exception happened for task with id {self._curr_task_id}. "
                             f"Updating status to {TaskStatus.FAILED}", exc_info=True)
                update_task_status(task_id=self._curr_task_id, status=TaskStatus.FAILED)
                failed = True
                raise
            finally:
                self.clean_on_exit(failed)

    @abstractmethod
    def launch_container(self) -> int:
       ...

    @abstractmethod
    def clean_on_exit(self, failed: bool):
        ...


class DockerBatchTaskExecutor(BatchTaskExecutor):
    def __init__(self, token: str,  curr_task_id: str, args: Dict[str, Any]):
        super().__init__(token, curr_task_id, args)
        self._container = None
        self._client = docker.from_env()

    def launch_container(self) -> int:
        if self.run_args.cpu is not None:
            logger.warning("Found cpu in args of task %s. "
                           "Currently we don't support cpu limits for docker based executor. "
                           "Ignoring it." % self._curr_task_id)

        # TODO: fix this expression
        volumes = [f"{os.environ[ENV_VAR_BATCH_WORKER_CONFIG_VOLUME]}:{DEFAULT_WORKER_CONFIG_PATH}"] \
            if ENV_VAR_BATCH_WORKER_CONFIG_VOLUME in os.environ else None

        logger.info("Checking if image pull is needed for task %s" % self._curr_task_id)
        try_pull_image(client=self._client, image=self.run_args.image)

        logger.info("Starting container for task %s" % self._curr_task_id)
        self._container = self._client.containers.run(
            detach=True,
            labels={
                LABEL_RN_ENTITY_TYPE: RNSEISM_BATCH_TASK,
                LABEL_RN_TASK_ID: self._curr_task_id,
                LABEL_RN_JOB_ID: self.run_args.job_id,
                LABEL_RN_PROJECT_ID: self.run_args.project_id,
                LABEL_RN_USER_ID: self.run_args.user_id
            },
            environment={
                **(self.run_args.environment or dict()),
                ENV_VAR_TOKEN: self.token,
                ENV_VAR_TASK_ID: self._curr_task_id,
                ENV_VAR_JOB_ID: self.run_args.job_id,
                ENV_VAR_RUNNER_DB_CONN: os.environ[ENV_VAR_RUNNER_DB_CONN],
                # ENV_VAR_CELERY_BROKER_URL: os.environ[ENV_VAR_CELERY_BROKER_URL],
                # ENV_VAR_CELERY_RESULT_BACKEND: os.environ[ENV_VAR_CELERY_RESULT_BACKEND]

                # HDFS credentials
                # TODO: uncomment imports and reference env var name variables from rnseism_sdk
                "WMS_HDFS_CLUSTER_NAME": os.getenv("WMS_HDFS_CLUSTER_NAME"),
                "WMS_HDFS_NAMENODE_HEAPSIZE": os.getenv("WMS_HDFS_NAMENODE_HEAPSIZE"),
                "WMS_HDFS_NN_RPC_URI": os.getenv("WMS_HDFS_NN_RPC_URI"),
                "WMS_HDFS_NN_HTTP_URI": os.getenv("WMS_HDFS_NN_HTTP_URI"),
                "WMS_HDFS_DATANODE_HOSTNAME": os.getenv("WMS_HDFS_DATANODE_HOSTNAME"),

                # SeismReader service URIs
                "SEISMREADER_COORDINATOR_HOST_PORT": os.getenv("SEISMREADER_COORDINATOR_HOST_PORT"),
                "SEISMREADER_RABBITMQ_HOST": os.getenv("SEISMREADER_RABBITMQ_HOST"),
                "SEISMREADER_RABBITMQ_PORT": os.getenv("SEISMREADER_RABBITMQ_PORT"),
                "SEISMREADER_WORKER_IDS": os.getenv("SEISMREADER_WORKER_IDS"),
                "SEISMREADER_WORKER_ADDRESSES": os.getenv("SEISMREADER_WORKER_ADDRESSES"),
            },
            volumes=volumes,
            network=os.environ.get(ENV_VAR_WORKER_NETWORK, None),
            image=self.run_args.image,
            command=self.run_args.command,
            entrypoint=self.run_args.entrypoint,
            mem_limit=self.run_args.memory
        )

        logger.info(f"Started container with id: {self._container.id}. Waiting for the completion...")

        self._container.wait()

        self._container = self._client.containers.get(self._container.id)
        exit_code = self._container.attrs['State']['ExitCode']

        logger.info(
            f"Container {self._container.id} finished "
            f"with exit status {self._container.status} and exit code {exit_code}")

        if exit_code != 0:
            raise TaskContainerFailed(f"Task container {self._container.id} "
                                      f"failed with exit status {self._container.status} and exit code {exit_code}")

        return exit_code

    def clean_on_exit(self, failed: bool):
        remove_policy = os.environ.get(
            ENV_VAR_BATCH_WORKER_REMOVE_CONTAINER_POLICY,
            ContainerRemovePolicy.always.value
        )
        remove_policy = ContainerRemovePolicy(remove_policy)

        remove_container = remove_policy.should_remove(failed)

        logger.info(f"Cleaning containers on exit for task with id {self._curr_task_id}")
        if self._container is not None:
            logger.info(f"Time limit exceeded or the task is being revoked. "
                        f"Stopping and removing container {self._container.id} in status {self._container.status}")
            self._container.stop(timeout=5)
            if remove_container:
                self._container.remove()
        else:
            found_containers: List[Container] \
                = self._client.containers.list(filters={'label': [f'{LABEL_RN_TASK_ID}={self._curr_task_id}']})

            if len(found_containers) > 1:
                logger.error(f"Found more than one container with task_id {self._curr_task_id} for removing. "
                             f"Will remove them all.")

            for cnt in found_containers:
                logger.info(f"Time limit exceeded or the task is being revoked. "
                            f"Stopping and removing container {cnt.id} in status {cnt.status}")
                cnt.stop(timeout=5)
                if remove_container:
                    cnt.remove()


class KubernetesBatchTasksExecutor(BatchTaskExecutor):
    def __init__(self,
                 token: str,
                 curr_task_id: str,
                 args: Dict[str, Any],
                 status_check_time_interval: float = 1,
                 timeout_to_get_running: float = 5,
                 termination_graceful_timeout: int = 5):
        super().__init__(token, curr_task_id, args)
        self._manager = current_kubernetes_manager()
        self._status_check_time_interval = status_check_time_interval
        self._timeout_to_get_running = timeout_to_get_running
        self._pod_name = f'batch-task-{self._curr_task_id}'
        self._termination_graceful_timeout = termination_graceful_timeout

    def launch_container(self) -> int:
        # TODO: workflow_config is not correct
        self._manager.launch_configured_pod(
            project_id=self.run_args.project_id,
            pod_name=self._pod_name,
            image=self.run_args.image,
            labels={
                **self._manager.base_labels(),
                LABEL_RN_ID: self._curr_task_id,
                LABEL_RN_TASK_ID: self._curr_task_id,
                LABEL_RN_TASK_TYPE: RNSEISM_BATCH_TASK,
                LABEL_RN_PROJECT_ID: self.run_args.project_id,
                LABEL_RN_JOB_ID: self.run_args.job_id,
                LABEL_RN_USER_ID: self.run_args.user_id
            },
            node_selector={
                LABEL_K8S_RN_OWNER: RNSEISM,
                LABEL_K8S_RN_NODE_TYPE_LABEL_KEY: NodeType.compute.value
            },
            command=self.run_args.entrypoint,
            args=self.run_args.command,
            env={
                **(self.run_args.environment or dict()),
                ENV_VAR_TOKEN: self.token,
                ENV_VAR_TASK_ID: self._curr_task_id,
                ENV_VAR_JOB_ID: self.run_args.job_id,
                # incorrect setting for this setup
                ENV_VAR_RUNNER_DB_CONN: os.environ.get(ENV_VAR_RUNNER_DB_CONN_EXTERNAL, os.environ[ENV_VAR_RUNNER_DB_CONN]),

                # HDFS credentials
                # TODO: uncomment imports and reference env var name variables from rnseism_sdk
                "WMS_HDFS_CLUSTER_NAME": os.getenv("WMS_HDFS_CLUSTER_NAME"),
                "WMS_HDFS_NAMENODE_HEAPSIZE": os.getenv("WMS_HDFS_NAMENODE_HEAPSIZE"),
                "WMS_HDFS_NN_RPC_URI": os.getenv("WMS_HDFS_NN_RPC_URI"),
                "WMS_HDFS_NN_HTTP_URI": os.getenv("WMS_HDFS_NN_HTTP_URI"),
                "WMS_HDFS_DATANODE_HOSTNAME": os.getenv("WMS_HDFS_DATANODE_HOSTNAME"),

                # SeismReader service URIs
                "SEISMREADER_COORDINATOR_HOST_PORT": os.getenv("SEISMREADER_COORDINATOR_HOST_PORT"),
                "SEISMREADER_RABBITMQ_HOST": os.getenv("SEISMREADER_RABBITMQ_HOST"),
                "SEISMREADER_RABBITMQ_PORT": os.getenv("SEISMREADER_RABBITMQ_PORT"),
                "SEISMREADER_WORKER_IDS": os.getenv("SEISMREADER_WORKER_IDS"),
                "SEISMREADER_WORKER_ADDRESSES": os.getenv("SEISMREADER_WORKER_ADDRESSES"),
            },
            cpu=self.run_args.cpu,
            memory=self.run_args.memory,
            termination_graceful_timeout=self._termination_graceful_timeout,
            volumes=self.run_args.volumes
        )

        exit_code = self._manager.wait_for_pod(pod_name=self._pod_name, timeout_to_get_running=30)
        return exit_code

    def clean_on_exit(self, failed: bool):
        remove_policy = os.environ.get(
            ENV_VAR_BATCH_WORKER_REMOVE_CONTAINER_POLICY,
            ContainerRemovePolicy.always.value
        )
        remove_policy = ContainerRemovePolicy(remove_policy)

        remove_container = remove_policy.should_remove(failed)

        logger.info(f"Cleaning containers on exit for task with id {self._curr_task_id}")

        try:
            pod = cast(
                V1Pod,
                self._manager.client.read_namespaced_pod(namespace=self._manager.namespace, name=self._pod_name)
            )
        except ApiException as ex:
            if ex.status == 404:
                return
            raise ex

        logger.info(f"Time limit exceeded or the task is being revoked. "
                    f"Stopping and removing container {self._pod_name} in status {pod.status.phase}")

        # if (remove_container and pod.status.phase == 'Failed') or pod.status.phase == 'Pending':
        if remove_container:
            self._manager.client.delete_namespaced_pod(
                namespace=self._manager.namespace,
                name=self._pod_name,
                grace_period_seconds=self._termination_graceful_timeout
            )

    @staticmethod
    def _extract_exit_code(pod: V1Pod) -> int:
        name = pod.metadata.name
        state = next(cstatus for cstatus in pod.status.container_statuses if cstatus.name == name).state
        assert state.terminated is not None
        state_terminated = cast(V1ContainerStateTerminated, state.terminated)
        return state_terminated.exit_code


class SparkOnK8sBatchTasksExecutor(KubernetesBatchTasksExecutor):
    @classmethod
    def parse_args(cls, args: Dict[str, Any]) -> SparkOnK8sBatchTaskRunArgs:
        return SparkOnK8sBatchTaskRunArgs.parse_obj(args)

    def launch_container(self) -> int:
        run_args = cast(SparkOnK8sBatchTaskRunArgs, self.run_args)

        if not self._manager.check_if_config_map_exists(run_args.k8s_spark_config_map_name):
            # todo custom exception
            raise ValueError(f"Spark config map with name {run_args.k8s_spark_config_map_name} doesn't exist")

        self._manager.launch_configured_pod(
            project_id=run_args.project_id,
            pod_name=self._pod_name,
            image=run_args.image,
            labels={
                **self._manager.base_labels(),
                LABEL_RN_ID: self._curr_task_id,
                LABEL_RN_TASK_ID: self._curr_task_id,
                LABEL_RN_TASK_TYPE: RNSEISM_BATCH_TASK,
                LABEL_RN_PROJECT_ID: run_args.project_id,
                LABEL_RN_JOB_ID: run_args.job_id,
                LABEL_RN_USER_ID: run_args.user_id
            },
            node_selector={
                LABEL_K8S_RN_OWNER: RNSEISM,
                LABEL_K8S_RN_NODE_TYPE_LABEL_KEY: NodeType.compute.value
            },
            command=self.run_args.entrypoint,
            args=self.run_args.command,
            env={
                **(self.run_args.environment or dict()),
                ENV_VAR_TOKEN: self.token,
                ENV_VAR_TASK_ID: self._curr_task_id,
                ENV_VAR_JOB_ID: self.run_args.job_id,
                ENV_VAR_RUNNER_DB_CONN: os.environ.get(ENV_VAR_RUNNER_DB_CONN_EXTERNAL, os.environ[ENV_VAR_RUNNER_DB_CONN]),
                "RNSEISM_SPARK_CLUSTER": "yes",
                "RNSEISM_SPARK_CONF_SPARK_KUBERNETES_DRIVER_POD_NAME": f"{self._pod_name}",
                "RNSEISM_SPARK_CONF_SPARK_KUBERNETES_DRIVER_MASTER": f"{self._pod_name}",
                "RNSEISM_SPARK_CONF_SPARK_DRIVER_HOST": f"{self._pod_name}",
                "RNSEISM_SPARK_CONF_SPARK_DRIVER_PORT": "39951",
                # may be in settings set by user
                # ENV_VAR_LOGGING_LEVEL
                # "RNSEISM_CODEGEN_STEP_IMPORTER_PATH": "['file:///usr/local/lib/python3.10/site-packages/rnseism_sdk/spark/custom/custom_processors.py']"

                # HDFS credentials
                # TODO: uncomment imports and reference env var name variables from rnseism_sdk
                "WMS_HDFS_CLUSTER_NAME": os.getenv("WMS_HDFS_CLUSTER_NAME"),
                "WMS_HDFS_NAMENODE_HEAPSIZE": os.getenv("WMS_HDFS_NAMENODE_HEAPSIZE"),
                "WMS_HDFS_NN_RPC_URI": os.getenv("WMS_HDFS_NN_RPC_URI"),
                "WMS_HDFS_NN_HTTP_URI": os.getenv("WMS_HDFS_NN_HTTP_URI"),
                "WMS_HDFS_DATANODE_HOSTNAME": os.getenv("WMS_HDFS_DATANODE_HOSTNAME"),

                "SEISMREADER_COORDINATOR_HOST_PORT": os.getenv("SEISMREADER_COORDINATOR_HOST_PORT"),
                "SEISMREADER_RABBITMQ_HOST": os.getenv("SEISMREADER_RABBITMQ_HOST"),
                "SEISMREADER_RABBITMQ_PORT": os.getenv("SEISMREADER_RABBITMQ_PORT"),
                "SEISMREADER_WORKER_IDS": os.getenv("SEISMREADER_WORKER_IDS"),
                "SEISMREADER_WORKER_ADDRESSES": os.getenv("SEISMREADER_WORKER_ADDRESSES"),
            },
            cpu=self.run_args.cpu,
            memory=self.run_args.memory,
            termination_graceful_timeout=self._termination_graceful_timeout,
            volumes=[
                {
                    'volume': {
                        'name': 'spark-config',
                        'configMap': {
                            'name': run_args.k8s_spark_config_map_name
                        }
                    },
                    'volumeMount': {
                        'name': 'spark-config',
                        'mountPath': '/etc/spark/conf',
                        'readOnly': True
                    }
                },
                *(self.run_args.volumes or [])
            ],
            service_type=run_args.service_type,
            service_ports=run_args.service_ports,
            service_account_name=run_args.service_account_name,
            # we use defaullt worker_config pre-created on the cluster
        )

        exit_code = self._manager.wait_for_pod(pod_name=self._pod_name, timeout_to_get_running=30)
        return exit_code


@shared_task
def run_docker_batch_task(token: str, curr_task_uid: str, args: Dict[str, Any]) -> Dict[str, Any]:
    return DockerBatchTaskExecutor(token, curr_task_uid, args).run()


@shared_task
def run_kubernetes_batch_task(token: str, curr_task_uid: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if 'task_type' not in args:
        raise ValueError("Arguments doesn't contain 'task_type' field")

    if args['task_type'] == TaskType.spark.value:
        return SparkOnK8sBatchTasksExecutor(token, curr_task_uid, args).run()

    return KubernetesBatchTasksExecutor(token, curr_task_uid, args).run()


# run_interactive_task.apply_async(args=[kwargs], queue='iw_team_a', routing_key='iw_team_a.run_interactive_task').get()
# to test from ipython console run: add.delay(4, 4).get()
# To test routing on different workers a and
# Run on A: add.apply_async(args=[4, 4], queue='qw_a', routing_key='a.add').get() # result: 16
# Run on B: add.apply_async(args=[4, 4], queue='qw_b', routing_key='b.add').get() # result: 24
# TODO: register parsers for pydantic entities
@shared_task
def run_interactive_task(token: str, curr_task_uid: str, args: Dict[str, Any]) -> Dict[str, Any]:
    run_args = InteractiveTaskRunArgs.parse_obj(args)

    logger.info(f"Received task {curr_task_uid} with args: {run_args.dict()}")

    # noinspection PyBroadException
    try:
        from geowsm.tasks.interactive_worker import current_runner

        update_task_status(task_id=curr_task_uid, status=TaskStatus.RUNNING)
        logger.debug(f"Updated status of task {curr_task_uid} to {TaskStatus.RUNNING}")

        result = current_runner().run(
            auth_token=token,
            task_id=curr_task_uid,
            job_id=run_args.job_id,
            session_id=run_args.session_id,
            **run_args.parameters
        )

        logger.info(f"Computed task {curr_task_uid} and received result: {result}")

        update_task_status(task_id=curr_task_uid, status=TaskStatus.FINISHED)
        logger.debug(f"Updated status of task {curr_task_uid} to {TaskStatus.FINISHED}")

        # TODO: add saving to results
        return TaskResult(task_id=curr_task_uid, result=result).dict()
    except RunnerException as ex:
        logger.warning(f"Task {curr_task_uid} failed. Updating status to {TaskStatus.FAILED}", exc_info=True)
        update_task_status(task_id=curr_task_uid, status=TaskStatus.FAILED)
        report_task_fail_reason(task_id=curr_task_uid, error_message=repr(ex), error_full=traceback.format_exc())
        raise
    except SoftTimeLimitExceeded as ex:
        logger.warning(f"Task {curr_task_uid} either was cancelled (by user) or exceeded time limit."
                       f" Updating status to {TaskStatus.CANCELLED}")
        update_task_status(task_id=curr_task_uid, status=TaskStatus.CANCELLED)
        report_task_fail_reason(task_id=curr_task_uid, error_message=repr(ex), error_full=traceback.format_exc())
        raise
    except Exception as ex:
        logger.error(f"Unexpected exception happened for task with id {curr_task_uid}. "
                     f"Updating status to {TaskStatus.FAILED}", exc_info=True)
        update_task_status(task_id=curr_task_uid, status=TaskStatus.FAILED)
        report_task_fail_reason(task_id=curr_task_uid, error_message=repr(ex), error_full=traceback.format_exc())
        raise
