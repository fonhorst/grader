from faststream import FastStream
from faststream.kafka import KafkaBroker
import os
import logging
import docker
import asyncio
from enum import Enum
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, cast
from docker.models.containers import Container
from kubernetes.client import V1Pod, V1ContainerStateTerminated, ApiException

from grader.db.tasks import TaskStatus, update_task_status, TaskType
from grader.env import ENV_VAR_BATCH_WORKER_TYPE, ENV_VAR_RUNNER_DB_CONN, ENV_VAR_CELERY_BROKER_URL, \
    ENV_VAR_CELERY_RESULT_BACKEND, ENV_VAR_BATCH_WORKER_CONFIG_VOLUME, DEFAULT_WORKER_CONFIG_PATH, ENV_VAR_TASK_ID, \
    ENV_VAR_JOB_ID, ENV_VAR_WORKER_NETWORK, ENV_VAR_BATCH_WORKER_REMOVE_CONTAINER_POLICY, \
    ENV_VAR_RUNNER_DB_CONN_EXTERNAL
from grader.tasks.base import ParametersManager, TaskResult, LABEL_TASK_ID, TaskContainerFailed, LABEL_ID, \
    LABEL_TASK_TYPE, GRADER_BATCH_TASK
from grader.tasks.batch_tasks_args import ContainerTaskRunArgs, SparkTaskRunArgs
from grader.tasks.kubernetes.kubernetes_manager import KubernetesManager, GRADER, LABEL_K8S_OWNER
from grader.tasks.utils import try_pull_image

# Other necessary imports from the original code...

logger = logging.getLogger(__name__)

broker = KafkaBroker()  # Using Kafka as an async broker

app = FastStream(broker)  # Defining FastStream app


class BatchWorkerType(Enum):
    docker = 'docker'
    kubernetes = 'kubernetes'


class AsyncKubernetesManager:
    def __init__(self, client):
        self.client = client
        self.namespace = "default"

    async def launch_configured_pod(self, pod_name: str, **kwargs):
        # Asynchronous Kubernetes pod creation
        # Example using aiohttp or async Kubernetes client
        await self.client.create_namespaced_pod(namespace=self.namespace, body=self._build_pod_spec(pod_name, **kwargs))

    async def wait_for_pod(self, pod_name: str, timeout_to_get_running: int) -> int:
        # Asynchronous wait logic for pod status updates
        while True:
            pod_status = await self.client.read_namespaced_pod_status(name=pod_name, namespace=self.namespace)
            if pod_status.status.phase == "Running":
                logger.info(f"Pod {pod_name} is running")
                break
            await asyncio.sleep(timeout_to_get_running)
        return await self._extract_exit_code(pod_name)

    async def check_if_config_map_exists(self, config_map_name: str) -> bool:
        # Check if a config map exists asynchronously
        try:
            await self.client.read_namespaced_config_map(name=config_map_name, namespace=self.namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise e

    async def delete_namespaced_pod(self, name: str, grace_period_seconds: int):
        # Async pod deletion
        await self.client.delete_namespaced_pod(name=name, namespace=self.namespace, grace_period_seconds=grace_period_seconds)



# Parameters and Kubernetes managers (same as original code but adapted for async use)
_current_parameters_manager: Optional[ParametersManager] = None
_current_kubernetes_manager: Optional[KubernetesManager] = None


def get_batch_worker_type() -> Optional[BatchWorkerType]:
    type = os.environ.get(ENV_VAR_BATCH_WORKER_TYPE, None)
    return BatchWorkerType(type) if type else None


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


@asynccontextmanager
async def parameters(task_uid: str, parameters: Dict[str, Any]):
    manager = current_parameters_manager()
    if not manager:
        raise ValueError("Parameters manager is not available, but required")

    manager.put(task_uid, parameters)

    yield

    manager.remove(task_uid)


class ContainerRemovePolicy(Enum):
    always = 'always'
    on_success = 'on_success'
    never = 'never'

    def should_remove(self, is_failed: bool) -> bool:
        if self == self.never:
            return False

        if (self == self.on_success) and is_failed:
            return False

        return True


class BatchTaskExecutor:
    @classmethod
    def parse_args(cls, args: Dict[str, Any]) -> ContainerTaskRunArgs:
        return ContainerTaskRunArgs.parse_obj(args)

    def __init__(self, curr_task_id: str, args: Dict[str, Any]):
        self._curr_task_id = curr_task_id
        self.run_args: ContainerTaskRunArgs = self.parse_args(args)

    async def run(self) -> Dict[str, Any]:
        logger.info(f"Received task with args: {self.run_args.dict()}")

        env_vars = [
            ENV_VAR_RUNNER_DB_CONN, ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND
        ]

        not_presented_env_vars = [env_var for env_var in env_vars if env_var not in os.environ]

        if len(not_presented_env_vars) > 0:
            raise ValueError(f"The following env vars are not set: {not_presented_env_vars}")

        async with parameters(self._curr_task_id, self.run_args.parameters):
            failed = False
            try:
                logger.info(f"Starting task {self._curr_task_id}. Updating status to {TaskStatus.RUNNING}")
                await update_task_status(task_id=self._curr_task_id, status=TaskStatus.RUNNING)

                logger.info("Launching container for task %s" % self._curr_task_id)
                await self.launch_container()

                logger.info(f"Finished task {self._curr_task_id}. Updating status to {TaskStatus.FINISHED}")
                await update_task_status(task_id=self._curr_task_id, status=TaskStatus.FINISHED)

                return TaskResult(task_id=self._curr_task_id, result=dict()).dict()
            except asyncio.TimeoutError:
                logger.warning(f"Soft time limit exceeded or the task ({self._curr_task_id}) was revoked. "
                               f"Updating status to {TaskStatus.CANCELLED}", exc_info=True)
                await update_task_status(task_id=self._curr_task_id, status=TaskStatus.CANCELLED)
                failed = True
                raise
            except Exception:
                logger.error(f"Unexpected exception happened for task with id {self._curr_task_id}. "
                             f"Updating status to {TaskStatus.FAILED}", exc_info=True)
                await update_task_status(task_id=self._curr_task_id, status=TaskStatus.FAILED)
                failed = True
                raise
            finally:
                await self.clean_on_exit(failed)

    async def launch_container(self) -> int:
        raise NotImplementedError

    async def clean_on_exit(self, failed: bool):
        raise NotImplementedError


class DockerBatchTaskExecutor(BatchTaskExecutor):
    def __init__(self, curr_task_id: str, args: Dict[str, Any]):
        super().__init__(curr_task_id, args)
        self._container = None
        self._client = docker.from_env()

    async def launch_container(self) -> int:
        if self.run_args.cpu is not None:
            logger.warning("Found CPU in args of task %s. "
                           "Currently we don't support CPU limits for Docker-based executor. Ignoring." % self._curr_task_id)

        volumes = [f"{os.environ[ENV_VAR_BATCH_WORKER_CONFIG_VOLUME]}:{DEFAULT_WORKER_CONFIG_PATH}"] \
            if ENV_VAR_BATCH_WORKER_CONFIG_VOLUME in os.environ else None

        logger.info(f"Checking if image pull is needed for task %s" % self._curr_task_id)
        await asyncio.to_thread(try_pull_image, self._client, self.run_args.image)  # Making Docker operations async

        logger.info(f"Starting container for task {self._curr_task_id}")
        self._container = await asyncio.to_thread(self._client.containers.run, detach=True, labels={
            LABEL_TASK_ID: self._curr_task_id,
        }, environment={
            **(self.run_args.environment or dict()),
            ENV_VAR_TASK_ID: self._curr_task_id,
            ENV_VAR_JOB_ID: self.run_args.job_id,
            ENV_VAR_RUNNER_DB_CONN: os.environ[ENV_VAR_RUNNER_DB_CONN]
        }, volumes=volumes, network=os.environ.get(ENV_VAR_WORKER_NETWORK, None),
        image=self.run_args.image, command=self.run_args.command, entrypoint=self.run_args.entrypoint,
        mem_limit=self.run_args.memory)

        logger.info(f"Started container with id: {self._container.id}. Waiting for the completion...")
        await asyncio.to_thread(self._container.wait)  # Waiting for container completion

        self._container = await asyncio.to_thread(self._client.containers.get, self._container.id)
        exit_code = self._container.attrs['State']['ExitCode']

        logger.info(f"Container {self._container.id} finished with exit status {self._container.status} and exit code {exit_code}")

        if exit_code != 0:
            raise TaskContainerFailed(f"Task container {self._container.id} failed with exit status {self._container.status} and exit code {exit_code}")

        return exit_code

    async def clean_on_exit(self, failed: bool):
        remove_policy = os.environ.get(ENV_VAR_BATCH_WORKER_REMOVE_CONTAINER_POLICY, ContainerRemovePolicy.always.value)
        remove_policy = ContainerRemovePolicy(remove_policy)

        remove_container = remove_policy.should_remove(failed)
        logger.info(f"Cleaning containers on exit for task with id {self._curr_task_id}")

        if self._container is not None:
            await asyncio.to_thread(self._container.stop, timeout=5)
            if remove_container:
                await asyncio.to_thread(self._container.remove)


class KubernetesBatchTasksExecutor(BatchTaskExecutor):
    def __init__(self,
                 curr_task_id: str,
                 args: Dict[str, Any],
                 status_check_time_interval: float = 1,
                 timeout_to_get_running: float = 5,
                 termination_graceful_timeout: int = 5):
        super().__init__(curr_task_id, args)
        self._manager = current_kubernetes_manager()
        self._status_check_time_interval = status_check_time_interval
        self._timeout_to_get_running = timeout_to_get_running
        self._pod_name = f'batch-task-{self._curr_task_id}'
        self._termination_graceful_timeout = termination_graceful_timeout

    async def launch_container(self) -> int:
        logger.info(f"Launching pod for task {self._curr_task_id}")
        await self._manager.launch_configured_pod(
            pod_name=self._pod_name,
            image=self.run_args.image,
            labels={
                **self._manager.base_labels(),
                LABEL_ID: self._curr_task_id,
                LABEL_TASK_ID: self._curr_task_id,
                LABEL_TASK_TYPE: GRADER_BATCH_TASK
            },
            node_selector={
                LABEL_K8S_OWNER: GRADER
            },
            command=self.run_args.entrypoint,
            args=self.run_args.command,
            env={
                **(self.run_args.environment or dict()),
                ENV_VAR_TASK_ID: self._curr_task_id,
                ENV_VAR_JOB_ID: self.run_args.job_id,
                ENV_VAR_RUNNER_DB_CONN: os.environ.get(ENV_VAR_RUNNER_DB_CONN_EXTERNAL,
                                                       os.environ[ENV_VAR_RUNNER_DB_CONN]),
            },
            cpu=self.run_args.cpu,
            memory=self.run_args.memory,
            termination_graceful_timeout=self._termination_graceful_timeout,
            volumes=self.run_args.volumes
        )

        logger.info(f"Waiting for pod {self._pod_name} to be ready...")
        exit_code = await self._manager.wait_for_pod(pod_name=self._pod_name,
                                                     timeout_to_get_running=self._timeout_to_get_running)

        logger.info(f"Pod {self._pod_name} finished with exit code {exit_code}")
        return exit_code

    async def clean_on_exit(self, failed: bool):
        remove_policy = os.environ.get(ENV_VAR_BATCH_WORKER_REMOVE_CONTAINER_POLICY, ContainerRemovePolicy.always.value)
        remove_policy = ContainerRemovePolicy(remove_policy)

        logger.info(f"Cleaning up pod {self._pod_name} for task {self._curr_task_id}")
        if remove_policy.should_remove(failed):
            try:
                await self._manager.client.delete_namespaced_pod(
                    namespace=self._manager.namespace,
                    name=self._pod_name,
                    grace_period_seconds=self._termination_graceful_timeout
                )
                logger.info(f"Pod {self._pod_name} removed successfully")
            except ApiException as ex:
                if ex.status == 404:
                    logger.warning(f"Pod {self._pod_name} not found, it might already be removed")
                else:
                    raise ex


class SparkOnK8sBatchTasksExecutor(KubernetesBatchTasksExecutor):
    @classmethod
    def parse_args(cls, args: Dict[str, Any]) -> SparkTaskRunArgs:
        return SparkTaskRunArgs.parse_obj(args)

    async def launch_container(self) -> int:
        run_args = cast(SparkTaskRunArgs, self.run_args)

        logger.info(f"Checking if Spark config map {run_args.k8s_spark_config_map_name} exists...")
        if not await self._manager.check_if_config_map_exists(run_args.k8s_spark_config_map_name):
            logger.error(f"Spark config map {run_args.k8s_spark_config_map_name} does not exist")
            raise ValueError(f"Spark config map with name {run_args.k8s_spark_config_map_name} doesn't exist")

        logger.info(f"Launching Spark pod {self._pod_name} for task {self._curr_task_id}")
        await self._manager.launch_configured_pod(
            pod_name=self._pod_name,
            image=run_args.image,
            labels={
                **self._manager.base_labels(),
                LABEL_ID: self._curr_task_id,
                LABEL_TASK_ID: self._curr_task_id,
                LABEL_TASK_TYPE: GRADER_BATCH_TASK
            },
            node_selector={
                LABEL_K8S_OWNER: GRADER
            },
            command=self.run_args.entrypoint,
            args=self.run_args.command,
            env={
                **(self.run_args.environment or dict()),
                ENV_VAR_TASK_ID: self._curr_task_id,
                ENV_VAR_JOB_ID: self.run_args.job_id,
                ENV_VAR_RUNNER_DB_CONN: os.environ.get(ENV_VAR_RUNNER_DB_CONN_EXTERNAL,
                                                       os.environ[ENV_VAR_RUNNER_DB_CONN]),
                "GRADER_SPARK_CLUSTER": "yes",
                "GRADER_SPARK_CONF_SPARK_KUBERNETES_DRIVER_POD_NAME": f"{self._pod_name}",
                "GRADER_SPARK_CONF_SPARK_KUBERNETES_DRIVER_MASTER": f"{self._pod_name}",
                "GRADER_SPARK_CONF_SPARK_DRIVER_HOST": f"{self._pod_name}",
                "GRADER_SPARK_CONF_SPARK_DRIVER_PORT": "39951",
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
        )

        logger.info(f"Waiting for Spark pod {self._pod_name} to be ready...")
        exit_code = await self._manager.wait_for_pod(pod_name=self._pod_name,
                                                     timeout_to_get_running=self._timeout_to_get_running)

        logger.info(f"Spark pod {self._pod_name} finished with exit code {exit_code}")
        return exit_code


# Define similar async logic for KubernetesBatchTasksExecutor and SparkOnK8sBatchTasksExecutor

# Now, use FastStream for task execution:
@app.subscriber("docker_task_topic")
async def run_docker_batch_task(message: Dict[str, Any]) -> Dict[str, Any]:
    curr_task_uid = message["curr_task_uid"]
    args = message["args"]
    return await DockerBatchTaskExecutor(curr_task_uid, args).run()


@app.subscriber("k8s_task_topic")
async def run_kubernetes_batch_task(message: Dict[str, Any]) -> Dict[str, Any]:
    curr_task_uid = message["curr_task_uid"]
    args = message["args"]

    if 'task_type' not in args:
        raise ValueError("Arguments don't contain 'task_type' field")

    if args['task_type'] == TaskType.spark.value:
        return await SparkOnK8sBatchTasksExecutor(curr_task_uid, args).run()

    return await KubernetesBatchTasksExecutor(curr_task_uid, args).run()

