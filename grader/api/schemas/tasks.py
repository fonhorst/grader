import enum
from typing import Dict, List, Union, Optional, Literal, Any

from pydantic import Field, root_validator
from pydantic.main import BaseModel
from rnseism.models.base import ValuesFilter
from rnseism_sdk.db.tasks import TaskStatus


class DateTimeRangeFilter(BaseModel):
    gt: Optional[Union[float, str]] = Field(None, alias="$gt")
    gte: Optional[Union[float, str]] = Field(None, alias="$gte")
    lt: Optional[Union[float, str]] = Field(None, alias="$lt")
    lte: Optional[Union[float, str]] = Field(None, alias="$lte")


# Schemes representing filtering
class TaskListFilter(BaseModel):
    uuid: Optional[ValuesFilter] = Field(None, description="filter expression on task uid")

    name: Optional[ValuesFilter] = Field(None, description="filter expression on task name")

    project_uid: Optional[ValuesFilter] = Field(None, description="filter expression on project uid")

    user_uid: Optional[ValuesFilter] = Field(None, description="filter expression on user uid")

    status: Optional[ValuesFilter] = Field(None, description="filter expression on status")

    submit_time: Optional[DateTimeRangeFilter] = Field(None, description="filter expression on submit time")

    task_type: Optional[ValuesFilter] = Field(None, description="filter expression on task type")


class TaskTunings(BaseModel):
    include_author: Optional[bool] = Field(
        None, description="include readable name of user or not", alias="includeAuthor"
    )

    include_project: Optional[bool] = Field(
        None, description="include info about the project the task belongs to", alias="includeProject"
    )

    include_nodes: Optional[bool] = Field(
        None, description="include info about nodes the task is running on", alias="includeNodes"
    )

    include_events: Optional[bool] = Field(
        None, description="include info about events happened to the task if available", alias="includeEvents"
    )

    include_reason: Optional[bool] = Field(
        None, description="include info about fail reason for failed tasks if available", alias="includeReason"
    )


class TaskRequest(BaseModel):
    name: Optional[str] = Field(
        None,
        description="name of the task",
        example="interpretation-server-replica-0"
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

    task_type: str = Field(
        ...,
        description="type of the task to run, affects how and where the task will be executed",
        example="batch"
    )

    job_id: str = Field(
        ...,
        description='a fully qualified name of the job to run;'
                    ' or a url-like custom pipeline docker image name'
                    ' ("graph://<image_name>" for code generation from graph and execution'
                    ' or "script://<image_name>" for execution of a compiled script)'
                    ', job_source field is required to work with graph or script',
        example='"geostat:cross_correlation", "graph://<image_name>", ...',
    )

    priority: Optional[float] = Field(
        None,
        description="a relative priority to be used for executing tasks",
        example="1024.0"
    )

    parameters: Dict[str, Any] = Field(
        ...,
        description="arguments to execute the function including data "
        "that should be delivered to the function from the managed cache/storage",
    )


class BatchTaskRequest(TaskRequest):
    task_type: Literal['batch']

    image: str = Field(
        ...,
        description="Docker image to run this task with",
        example="some_batch_image:latest"
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


class SparkOnK8sTaskRequest(BatchTaskRequest):
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


# Supplementary info
class ResourceUnitsEnum(enum.Enum):
    cores = "CORES"
    gigabytes = "GIGABYTES"


class ResourceInfo(BaseModel):
    """
    Describes resource usage.
    Here fully_used <= used <= available
    """

    fully_used: Optional[Union[int, float]] = Field(
        ..., description="the amount of fully used resources", alias="fullyUsed"
    )

    used: Union[int, float] = Field(
        ..., description="the amount of used resources", alias="used"
    )
    available: Union[int, float] = Field(
        ..., description="the amount of available resources", alias="available"
    )
    units: ResourceUnitsEnum = Field(..., description="the units of resources")

    class Config:
        smart_union = True
        allow_population_by_field_name = True

    @root_validator
    def validate_units_type(cls, values):
        if values["units"] == ResourceUnitsEnum.cores and (
            isinstance(values["used"], float) or isinstance(values["available"], float)
        ):
            raise ValueError("Cores should be integer")
        return values


class CpuAndRamUsageInfo(BaseModel):
    """
    Combines CPU and RAM usage.
    """

    cpu: Optional[ResourceInfo] = Field(..., description="CPU usage")

    ram: Optional[ResourceInfo] = Field(..., description="RAM usage")


class NodeInfo(BaseModel):
    # TODO: implement uuid processing
    uuid: str = Field("00000000-0000-0000-0000-000000000000", description="Node uuid")
    name: str = Field(..., description="Node name")

    ip: Optional[str] = Field(None, description="Node ip if available")

    is_priority_node: bool = Field(..., description="Node priority sign", alias="isPriorityNode")

    status: TaskStatus = Field(..., description="Node's task status")
    estimated_completion_time: Optional[float] = Field(None, description="Remaining time estimation, sec")

    cpu: ResourceInfo = Field(..., description="CPU resources")
    gpu: Optional[ResourceInfo] = Field(None, description="GPU resources")
    ram: ResourceInfo = Field(..., description="Memory resources")

    class Config:
        allow_population_by_field_name = True


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


class TaskEventType(enum.Enum):
    created = "CREATED"
    started = "STARTED"
    ended = "ENDED"


class TaskEvent(BaseModel):
    event_type: TaskEventType = Field(..., description="The type of the event")

    timestamp: float = Field(..., description="Timestamp of the event as a POSIX timestamp")


class TaskInfoResponse(TaskRequest):
    uuid: Optional[str] = Field(..., description="task unique identifier", example="interpretation-server-replica-0")

    user_id: str = Field(
        ...,
        description="id of the user who created the task",
        example="some user id"
    )

    project_id: str = Field(
        ...,
        description="id of the project this task belongs to",
        example="interpretation-server-replica-0"
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

    progress: float = Field(
        ...,
        description="Current progress value",
        example="35.67"
    )

    progress_message: Optional[str] = Field(
        None,
        description="Current progress  message",
        example="Performing correlation post-processing"
    )

    duration: Optional[float] = Field(
        None,
        description="Current duration of the task. "
                    "If the task has one of terminal statuses, "
                    "duration shows passed time from the submit event to the end event . "
                    "If the task doesn't have a terminal status, "
                    "duration shows passed time from the submit event to the current moment.",
        example="TBD"
    )

    estimated_completion_time: Optional[float] = Field(
        None,
        description="the task estimated completion time as a POSIX timestamp",
        example="TBD"
    )

    reason: Optional[str] = Field(
        None,
        description="the reason of fail if task failed or cancelled (may be not available in some situations)",
        example="ValueError('Incorrect key value')"
    )

    author_name: Optional[str] = Field(
        None,
        description="Readble name of user who submitted the task",
        example="TBD"
    )

    project_name: Optional[str] = Field(
        None,
        description="Name of the project the task belongs to",
        example="TBD"
    )

    project_info: Optional[str] = Field(
        None,
        description="Description of the project the task belongs to",
        example="TBD"
    )

    nodes: Optional[List['NodeInfo']] = Field(
        None,
        description="Nodes the task uses for computations",
        example="TBD"
    )

    nodes_count: Optional[int] = Field(
        None,
        description="Count of nodes in the task."
                    " If \"nodes\" arg passed not None, nodes_count=nodes.count(),"
                    " else use either passed or default value."
    )

    events: Optional[List[TaskEvent]] = Field(
        None,
        description="Events related to the task lifecycle",
        example="TBD"
    )

    @root_validator
    def init_nodes_count(cls, kwargs):
        # If nodes present not as None, count list len, else use either passed or default value (None)
        if (x := kwargs.get("nodes", None)) is not None:
            kwargs["nodes_count"] = len(x)
        return kwargs


class TaskResultResponse(BaseModel):
    uid: str = Field(..., description="task unique identifier", example="interpretation-server-replica-0")

    results: Optional[Dict[str, Any]] = Field(
        None,
        description="Dictionary containing all results produced by task and written to task result storage. "
                    "If task is not yet finished or failed or has been cancelled, there may be no results. "
                    "Also, partial results may be available if task has failed "
                    "or has been cancelled in the middle of execution.",
        example="TBD"
    )


class TaskLogResponse(BaseModel):
    uid: str = Field(..., description="task unique identifier", example="interpretation-server-replica-0")

    log: Optional[str] = Field(
        None,
        description="Log entries for the task if available",
        example="TBD"
    )


# Descriptive entities
class ComputeFunctionArgInfo(BaseModel):
    name: str = Field(..., description="Name of a function argument")

    description: str = Field(..., description="Description of a function argument")

    examples: List[str] = Field(
        ..., description="Examples of possible values for this argument"
    )


class ComputableFunction(BaseModel):
    uid: str = Field(
        ...,
        description="a fully qualified name of the function to run",
        example="geostat:cross_correlation",
    )

    name: str = Field(
        ...,
        description="user-friendly name of the function",
        example="Cross correlation for slices",
    )

    description: str = Field(
        ...,
        description="user-friendly description of the function",
        example="Something that describes how this function works",
    )

    args: Optional[List[ComputeFunctionArgInfo]] = Field(
        ..., description="Description of the function arguments"
    )

    outputs: Optional[ComputeFunctionArgInfo] = Field(
        ..., description="Description of the function outputs"
    )


AsyncCapableTasks = Union[
    BatchTaskRequest,
    InteractiveTaskRequest,
    SparkOnK8sTaskRequest
]


SyncCapableTasks = Union[
    ImmediateTaskRequest,
    AsyncCapableTasks
]
