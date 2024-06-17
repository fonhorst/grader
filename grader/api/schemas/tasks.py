import enum
import operator
from typing import Dict, List, Union, Optional, Literal, Any

from pydantic import Field
from pydantic.main import BaseModel


class DateTimeRangeFilter(BaseModel):
    gt: Optional[Union[float, str]] = Field(None, alias="$gt")
    gte: Optional[Union[float, str]] = Field(None, alias="$gte")
    lt: Optional[Union[float, str]] = Field(None, alias="$lt")
    lte: Optional[Union[float, str]] = Field(None, alias="$lte")


class ValuesFilter(BaseModel):
    eq: Optional[Union[float, int, str]] = Field(None, alias="$eq")
    ne: Optional[Union[float, int, str]] = Field(None, alias="$ne")
    gt: Optional[Union[float, int]] = Field(None, alias="$gt")
    gte: Optional[Union[float, int]] = Field(None, alias="$gte")
    lt: Optional[Union[float, int]] = Field(None, alias="$lt")
    lte: Optional[Union[float, int]] = Field(None, alias="$lte")
    in_values: Optional[List[Union[float, int, str]]] = Field(None, alias="$in")
    not_in_values: Optional[List[Union[float, int, str]]] = Field(None, alias="$nin")
    regexp: Optional[str] = Field(None, alias="$regex")

    def get_range_min(self):
        return self._get_boundary(f=operator.ge, attr1="gt", attr1_incl="gte")

    def get_range_max(self):
        return self._get_boundary(f=operator.le, attr1="lt", attr1_incl="lte")

    def _get_boundary(self, f, attr1, attr1_incl):
        current = None
        include_value = False

        v1 = getattr(self, attr1)
        v1_incl = getattr(self, attr1_incl)

        if v1 is not None:
            current = v1

        if v1_incl is not None:
            if current is not None:
                if f(v1_incl, v1):
                    current = v1_incl
                    include_value = True
            else:
                current = v1_incl
                include_value = True

        return current, include_value


# Schemes representing filtering
class TaskListFilter(BaseModel):
    uid: Optional[ValuesFilter] = Field(None, description="filter expression on task uid")

    name: Optional[ValuesFilter] = Field(None, description="filter expression on task name")

    requester: Optional[ValuesFilter] = Field(None, description="filter expression on project uid")

    student: Optional[ValuesFilter] = Field(None, description="filter expression on project uid")

    project: Optional[ValuesFilter] = Field(None, description="filter expression on project uid")

    tag: Optional[ValuesFilter] = Field(None, description="filter expression on project uid")

    task_type: Optional[ValuesFilter] = Field(None, description="filter expression on task type")

    job_id: Optional[ValuesFilter] = Field(None, description="filter expression on task type")

    status: Optional[ValuesFilter] = Field(None, description="filter expression on status")

    submit_time: Optional[DateTimeRangeFilter] = Field(None, description="filter expression on submit time")


class TaskRequest(BaseModel):
    name: Optional[str] = Field(
        None,
        description="name of the task",
        example="Submission Lab #1"
    )

    requester: str = Field(
        None,
        description="Name or uid of the one who submitted the task",
        example="tutor"
    )

    student: str = Field(
        None,
        description="Name or uid of the one the task was submitted for",
        example="apetrov"
    )

    project: str = Field(
        None,
        description="Name of the project or course name the student belongs to",
        example="bigdata"
    )

    tag: str = Field(
        None,
        description="arbitrary tag to associate with the task",
        example="fall'24"
    )

    task_type: str = Field(
        ...,
        description="type of the task to run, affects how and where the task will be executed",
        example="regular"
    )

    job_id: str = Field(
        ...,
        description='a fully qualified name of the checking job to run',
        example='container://b6da673d116f41778cc634e101cb0b17',
    )

    priority: Optional[float] = Field(
        None,
        description="a relative priority for this task",
        example="1024.0"
    )

    parameters: Dict[str, Any] = Field(
        ...,
        description="arguments to execute the job"
    )


class BatchTaskRequest(TaskRequest):
    task_type: Literal['batch']

    image: str = Field(
        ...,
        description="Docker image to run this task with",
        example="some_image:latest"
    )

    environment: Optional[Dict[str, str]] = Field(
        None,
        description="env vars that can be set by a user",
        example="{'CELERY_BROKER_URL': 'redis:5050'}"
    )

    entrypoint: Optional[str] = Field(
        None,
        description="Executable file to use when a task container is created.",
        example="/bin/bash"
    )

    command: Optional[List[str]] = Field(
        None,
        description="Set of arguments to the executable of the image.",
        example="['--in', '/some/file/path']"
    )

    cpu: Optional[int] = Field(
        None,
        description="Number of cores to allocate for the task",
        example="1"
    )

    memory: Optional[int] = Field(
        None,
        description="Memory to allocate for the task (in megabytes)",
        example="1024"
    )

    time_limit: Optional[float] = Field(
        None,
        description="maximum time limit for calculating this function (soft)",
        example="3600.0"
    )

    volumes: Optional[Dict[str, Any]] = Field(
        None,
        description="Only for k8s as of now. Mount entrypoints to be mounted on batch tasks pods.  "
                    "A dict that should contain 'volumes' and 'volumeNounts' fields with content "
                    "that k8s can understand.",
        example="""{
            "volumes": [
                {
                    'name': 'worker-config',
                    'configMap': {
                        'name': config_map_name,
                        'items': [{'key': WORKER_CONFIG_CONFIG_MAP_KEY, 'path': WORKER_CONFIG_CONFIG_MAP_KEY}]
                    }
                },
            ],
            "volumeMounts":[
                {
                    'name': 'worker-config',
                    'mountPath': DEFAULT_WORKER_CONFIG_PATH,
                    'subPath': WORKER_CONFIG_CONFIG_MAP_KEY,
                    'readOnly': True
                }
            ]
        }"""
    )


class SparkTaskRequest(BatchTaskRequest):
    task_type: Literal['spark']

    k8s_spark_config_map_name: str = Field(
        "spark-conf",
        description="The name of config map that exists in the k8s's namespace where spark task will be launched. "
                    "The configmap should be pre-created."
                    "This config map should contain a content of spark-defaults.conf "
                    "with appropriate settings to run a spark app on this cluster.",
        example="spark-conf"
    )

    service_type: str = Field(
        'ClusterIP',
        description="Type of the service that will be created to forward connections to spark driver. "
                    "May be either ClusterIP or NodeType. NodeType may allow to access spark driver WebUI on k8s",
        example="ClusterIP"
    )

    service_ports: List[int] = Field(
        [4040, 39951, 39570],
        description="Service ports which the service will forward to the spark driver. "
                    "DO NOT touch this settings unless you know what you are doing."
                    "Used by spark executors and in some cases a user "
                    "that externally access the spark driver (port 4040).",
        example="[4040, 39951, 39570]"
    )

    service_account_name: str = Field(
        'spark',
        description="A service account that will be used by spark driver to ask K8s to create its executors. "
                    "The service account should be pre-created by the cluster administrator.",
        example="3600.0"
    )


# TaskInfo object and related entities
class TaskStatus(enum.Enum):
    created = "created"
    running = "running"
    cancelling = "cancelling"
    finished = "finished"
    failed = "failed"
    cancelled = "cancelled"

    def is_terminal(self):
        return self in [TaskStatus.failed, TaskStatus.finished, TaskStatus.failed]


class TaskInfoResponse(TaskRequest):
    uid: Optional[str] = Field(
        ...,
        description="task unique identifier",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    )

    status: TaskStatus = Field(
        ...,
        description="Status of the task at the current moment",
        example="running"
    )

    submit_time: str = Field(
        ...,
        description="submit time in iso format",
        example="TBD"
    )

    end_time: Optional[str] = Field(
        None,
        description="end time in iso format",
        example="TBD"
    )

    reason: Optional[str] = Field(
        None,
        description="the reason of fail if task failed or cancelled (may be not available in some situations)",
        example="ValueError('Incorrect key value')"
    )


class TaskResultResponse(BaseModel):
    uid: str = Field(
        ...,
        description="task unique identifier",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    )

    results: Optional[Dict[str, Any]] = Field(
        None,
        description="Dictionary containing all results produced by task, "
                    "including the ones written into HDFS and represented as paths there."
                    "If task is not yet finished or failed or has been cancelled, there may be no results. "
                    "Also, partial results may be available if task has failed "
                    "or has been cancelled in the middle of execution.",
        example="TBD"
    )


class TaskLogResponse(BaseModel):
    uid: str = Field(
        ...,
        description="task unique identifier",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    )

    log: Optional[str] = Field(
        None,
        description="Log entries for the task if available",
        example="TBD"
    )


AvailableTaskTypes = Union[
     TaskRequest,
     BatchTaskRequest,
     SparkTaskRequest
]
