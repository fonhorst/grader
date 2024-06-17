import enum
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List, Optional, Union, Tuple

from celery.result import AsyncResult
from pydantic import BaseModel
from rnseism.models.base import Navigation

from rnseism_sdk.db.tasks import Task, list_tasks, get_task, TaskStatus, TaskType, DateTimeType
from rnseism_sdk.sdk.base import DataStorage

RNSEISM_INTERACTIVE_WORKER = 'interactive-worker'
RNSEISM_BATCH_WORKER = 'batch-worker'
RNSEISM_BATCH_TASK = 'batch-task'


LABEL_RN_ID = 'rn_id'
LABEL_RN_ENTITY_TYPE = 'rn_entity_type'
LABEL_RN_ENTITY_NAME = 'rn_entity_name'
LABEL_RN_TASK_ID = 'rn_task_id'
LABEL_RN_TASK_TYPE = 'rn_task_type'
LABEL_RN_PROJECT_ID = 'rn_project_id'
LABEL_RN_STORAGE_ID = 'rn_storage_id'
LABEL_RN_JOB_ID = 'rn_job_id'
LABEL_RN_USER_ID = 'rn_user_id'


TaskUUID = Union[str, uuid.UUID]

ComplexUID = Tuple[str, str]


def make_complex_uid(task_type: Union[str, TaskType], uid: TaskUUID) -> str:
    tt = task_type.value if isinstance(task_type, TaskType) else task_type
    return f"{tt}--{uid}"


def split_complex_uid(uid: str) -> ComplexUID:
    task_type, task_uuid = uid.split("--")
    return task_type, task_uuid


def validate_and_split_complex_uid(uid: str, raise_exc: bool = True) -> Optional[ComplexUID]:
    result = uid.split("--")

    if len(result) == 2:
        try:
            task_type, uid = result
            uuid.UUID(uid)
            TaskType(task_type)
        except ValueError:
            result = None
    else:
        result = None

    if result is None and raise_exc:
        raise ValueError("Invalid uid. Uid should be in the form <task_type>--<UUID>")
    elif result is None:
        return None

    task_type, uid = result
    return task_type, uid


class TaskInfo(BaseModel):
    @staticmethod
    def from_task(task: Task,
                  estimated_completion_time: Optional[datetime] = None) -> 'TaskInfo':
        return TaskInfo(
            uid=make_complex_uid(task.task_type, task.id),
            task_uid=str(task.id),
            name=task.name,
            task_type=TaskType(task.task_type),
            user_id=str(task.user_id),
            project_id=str(task.project_id),
            job_id=task.job_id,
            priority=task.priority,
            parameters=task.parameters,
            status=TaskStatus(task.status),
            status_updated_at=task.status_updated_at,
            submit_time=task.submit_time,
            end_time=task.end_time,
            progress=task.progress,
            progress_message=task.progress_message,
            metrics=task.metrics,
            duration=(task.end_time - task.submit_time).seconds if task.end_time else
                (datetime.now() - task.submit_time).seconds,
            estimated_completion_time=estimated_completion_time,
            reason=(task.reason.error_full or task.reason.error_message) if task.reason else None
        )
    uid: str
    task_uid: str
    name: str
    task_type: TaskType
    user_id: str
    project_id: str
    job_id: str
    priority: float
    parameters: Dict[str, Any]
    status: TaskStatus
    status_updated_at: datetime
    submit_time: datetime
    end_time: Optional[datetime] = None
    progress: float
    progress_message: Optional[str]
    metrics: Optional[Dict[str, Union[str, int, float, bool]]] = None
    duration: float
    estimated_completion_time: Optional[datetime] = None
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
    user_id: str
    project_id: str
    job_id: str
    priority: float = 0.0
    parameters: Dict[str, Any]
    context: Optional[Dict[str, Any]] = None


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


class NodeType(enum.Enum):
    interactive = 'interactive'
    compute = 'compute'
