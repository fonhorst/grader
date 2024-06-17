from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List, Optional, Union, Tuple

from celery.result import AsyncResult
from pydantic import BaseModel
from rnseism.models.base import Navigation

from grader.db.tasks import Task, TaskStatus, TaskType, list_tasks, get_task

# from rnseism_sdk.db.tasks import Task, list_tasks, get_task, TaskStatus, TaskType, DateTimeType
# from rnseism_sdk.sdk.base import DataStorage

GRADER_BATCH_WORKER = 'batch-worker'
GRADER_BATCH_TASK = 'batch-task'


LABEL_ID = 'grader_id'
LABEL_ENTITY_TYPE = 'grader_entity_type'
LABEL_ENTITY_NAME = 'grader_entity_name'
LABEL_TASK_ID = 'grader_task_id'
LABEL_TASK_TYPE = 'grader_task_type'
LABEL_PROJECT_ID = 'grader_project_id'
LABEL_STORAGE_ID = 'grader_storage_id'
LABEL_JOB_ID = 'grader_job_id'
LABEL_USER_ID = 'grader_user_id'


class TaskInfo(BaseModel):
    @staticmethod
    def from_task(task: Task) -> 'TaskInfo':
        return TaskInfo(
            uid=task.id,
            name=task.name,
            task_type=TaskType(task.task_type),
            requester=task.requester,
            student=task.student,
            project=task.project,
            tag=task.tag,
            job_id=task.job_id,
            priority=task.priority,
            parameters=task.parameters,
            status=TaskStatus(task.status),
            status_updated_at=task.status_updated_at,
            submit_time=task.submit_time,
            end_time=task.end_time,
            metrics=task.metrics,
            reason=(task.reason.error_full or task.reason.error_message) if task.reason else None
        )
    uid: str
    task_uid: str
    name: str
    task_type: TaskType
    requester: str
    student: str
    project: str
    tag: str
    job_id: str
    priority: float
    parameters: Dict[str, Any]
    status: TaskStatus
    status_updated_at: datetime
    submit_time: datetime
    end_time: Optional[datetime] = None
    metrics: Optional[Dict[str, Union[str, int, float, bool]]] = None
    reason: Optional[str] = None


@dataclass
class TaskAndResult:
    task: TaskInfo
    result: AsyncResult


class TaskLog(BaseModel):
    task_id: str
    log: str


class TaskResult(BaseModel):
    task_id: str
    result: Any


class TaskRunArgs(BaseModel):
    name: Optional[str]
    task_type: str
    requester: str
    student: str
    project: str
    tag: str
    job_id: str
    priority: float = 0.0
    parameters: Dict[str, Any]


class TasksManager(ABC):
    task_types: List[str]
    result_storage: DataStorage

    def list(self,
             uuids: Optional[List[str]] = None,
             project_ids: Optional[List[str]] = None,
             author_ids: Optional[List[str]] = None,
             statusess: Optional[List[str]] = None,
             task_types: Optional[List[str]] = None,
             submit_time: Optional[Tuple[DateTimeType, DateTimeType]] = None,
             name: Optional[Union[str, List[str]]] = None,
             navigation: Optional[Navigation] = None,
             include_reason: bool = False) -> List[TaskInfo]:
        return [
            TaskInfo.from_task(task) for task in list_tasks(
                uuids=uuids,
                task_types=task_types or self.task_types,
                project_ids=project_ids,
                author_ids=author_ids,
                statusess=statusess,
                submit_time=submit_time,
                name=name,
                navigation=navigation,
                include_reason=include_reason
            )
        ]

    def get(self, uid: str, include_reason: bool = False) -> TaskInfo:
        task = get_task(uid, include_reason)
        return TaskInfo.from_task(task)

    def get_result(self, uid: str) -> Optional[Any]:
        task_prefix = f"{uid}."

        def check_key(key: str):
            return key.startswith(task_prefix)

        results = self.result_storage.list(prefix=uid)
        task_results = {key[len(task_prefix):]: value for key, value in results.items() if check_key(key)} \
            if len(results) > 0 else None

        return task_results

    def get_log(self, uid: str, tail: Optional[int] = None) -> Optional[str]:
        return None

    @abstractmethod
    def start(self, token: str, args: TaskRunArgs) -> TaskAndResult:
        ...

    @abstractmethod
    def stop(self, uid: str):
        ...


class TaskContainer(Exception):
    pass


class TaskContainerFailed(TaskContainer):
    pass


class TaskContainerExecutionTimeout(TaskContainer):
    pass
