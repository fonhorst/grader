import datetime
import logging
import uuid
from abc import abstractmethod
from dataclasses import dataclass
from typing import cast, Optional

import celery
import docker
from celery.result import AsyncResult
from kubernetes import client, config

from rnseism_sdk.db.tasks import create_task, update_task_status, TaskStatus, TaskType
from rnseism_sdk.sdk.base import DataStorage
from grader.tasks.app import BATCH_TASKS_QUEUE, make_app, KUBERNETES_BATCH_TASKS_QUEUE
from grader.tasks.base import TasksManager, TaskAndResult, TaskInfo, LABEL_ENTITY_TYPE, \
    GRADER_BATCH_TASK, LABEL_TASK_ID, LABEL_TASK_TYPE
from grader.tasks.batch_tasks_args import BatchTaskRunArgs
from grader.tasks.tasks import run_docker_batch_task, run_kubernetes_batch_task
from grader.tasks.utils import get_docker_container_logs, get_kubernetes_container_logs

logger = logging.getLogger()


BATCH_TASK_TYPE = 'batch'

app = make_app()


@dataclass
class _BatchQueueSettings:
    queue: str
    routing_key: str
    run_func: celery.Task


class BatchTasksManager(TasksManager):
    task_types = [BATCH_TASK_TYPE]

    def __init__(self, result_storage: DataStorage) -> None:
        super().__init__()
        self.result_storage = result_storage

    def start(self, token: str, args: BatchTaskRunArgs) -> TaskAndResult:
        run_call = args.dict()

        task_id = uuid.uuid4()

        task = create_task(
            uid=task_id,
            name=args.name,
            task_type=TaskType(args.task_type),
            user_id=args.user_id,
            project_id=args.project_id,
            job_id=args.job_id,
            priority=args.priority,
            parameters=args.parameters,
            submit_time=datetime.datetime.now()
        )

        qs = self._get_queue_settings(args)

        ctask = qs.run_func
        ctask.bind(app)
        awaitable_result: AsyncResult = ctask.apply_async(
            args=[token, str(task_id), run_call],
            task_id=str(task_id),
            queue=qs.queue,
            routing_key=qs.routing_key
        )
        return TaskAndResult(TaskInfo.from_task(task), awaitable_result)

    def stop(self, uid: str):
        # status - Cancelled
        app.control.revoke(uid, terminate=True)
        update_task_status(uid, status=TaskStatus.CANCELLED)

    @abstractmethod
    def _get_queue_settings(self, args) -> _BatchQueueSettings:
        ...


class DockerBatchTasksManager(BatchTasksManager):
    def __init__(self, result_storage: DataStorage) -> None:
        super().__init__(result_storage)
        self._client = docker.from_env()

    def _get_queue_settings(self, args) -> _BatchQueueSettings:
        return _BatchQueueSettings(
            queue=BATCH_TASKS_QUEUE,
            routing_key=f'{BATCH_TASKS_QUEUE}.run_docker_batch_task',
            run_func=cast(celery.Task, run_docker_batch_task),
        )

    def get_log(self, uid: str, tail: Optional[int] = None) -> Optional[str]:
        # _, task_uid = split_complex_uid(uid)
        labels = [
            f'{LABEL_TASK_ID}={uid}',
            f'{LABEL_ENTITY_TYPE}={GRADER_BATCH_TASK}'
        ]
        return get_docker_container_logs(self._client, labels, tail)


class KubernetesBatchTasksManager(BatchTasksManager):
    task_types = [BATCH_TASK_TYPE, 'spark']

    def __init__(self, result_storage: DataStorage, namespace: str) -> None:
        super().__init__(result_storage)
        self.namespace = namespace
        config.load_kube_config()
        self._client = client.CoreV1Api()

    def _get_queue_settings(self, args) -> _BatchQueueSettings:
        return _BatchQueueSettings(
            queue=KUBERNETES_BATCH_TASKS_QUEUE,
            routing_key=f'{KUBERNETES_BATCH_TASKS_QUEUE}.run_kubernetes_batch_task',
            run_func=cast(celery.Task, run_kubernetes_batch_task)
        )

    def get_log(self, uid: str, tail: Optional[int] = None) -> Optional[str]:
        # _, task_uid = split_complex_uid(uid)

        labels = [
            f'{LABEL_TASK_ID}={uid}',
            f'{LABEL_TASK_TYPE}={GRADER_BATCH_TASK}'
        ]

        return get_kubernetes_container_logs(self._client, self.namespace, labels, tail)
