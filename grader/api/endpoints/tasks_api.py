import datetime
import logging
import pprint
from typing import Optional, List, Dict, Any, Union

from fastapi import Body, Path, Query

from grader.api.base import tasks_manager
from grader.api.schemas import tasks
from grader.api.schemas.tasks import AvailableTaskTypes
from grader.api.schemas.tasks import TaskListFilter, TaskInfoResponse, TaskResultResponse, TaskLogResponse, \
    TaskStatus
from grader.app import app
from grader.db.tasks import TaskType
from grader.tasks.base import TaskInfo, TaskRunArgs
from grader.tasks.batch_tasks_args import ContainerTaskRunArgs, SparkTaskRunArgs

METHOD_PREFIX = "Task"


logger = logging.getLogger(__name__)


def format_datetime(dt: Optional[Union[float, datetime.datetime]]) -> Optional[str]:
    if not dt:
        return None

    if isinstance(dt, float):
        return datetime.datetime.fromtimestamp(dt).isoformat()

    return dt.isoformat()


def convert_task_info_to_response(task: TaskInfo) -> TaskInfoResponse:
    task_response = TaskInfoResponse(
        **task.dict(exclude={'uid', 'task_type', 'status', 'status_updated_at', 'submit_time', 'end_time'}),
        uuid=task.uid,
        submit_time=format_datetime(task.submit_time),
        end_time=format_datetime(task.end_time),
        task_type=task.task_type.value,
        status=TaskStatus(task.status.value),
    )
    return task_response


def handle_filter(filter: TaskListFilter) -> Dict[str, Any]:
    kwargs = dict()

    if filter.submit_time:
        if filter.submit_time.gte and filter.submit_time.gt:
            raise ValueError(f"Either gte or gt should be provided, not both")

        if filter.submit_time.lte and filter.submit_time.lt:
            raise ValueError(f"Either lte or lt should be provided, not both")

        kwargs['submit_time'] = (filter.submit_time.gte or filter.submit_time.gt,
                                 filter.submit_time.lt or filter.submit_time.lte)

    ffields =[
        ('uid', 'uids', filter.uid),
        ('name', 'name', filter.name),
        ('requester', 'requester', filter.requester),
        ('student', 'student', filter.student),
        ('project', 'project', filter.project),
        ('tag', 'tag', filter.tag),
        ('job_id', 'job_ids', filter.job_id),
        ('status', 'statusess', filter.status),
        ('task_type', 'task_types', filter.task_type)
    ]
    for field_name, arg_name, vfilter in ffields:
        if not vfilter:
            continue

        unsupported_filter_ops_cond = \
            all(
                el is None for el in
                [vfilter.ne, vfilter.gt, vfilter.gte, vfilter.lt, vfilter.lte, vfilter.not_in_values, vfilter.regexp]
            )

        if not unsupported_filter_ops_cond:
            raise ValueError(f"Found unsupported filter operation for {field_name}. "
                             f"Only the following ops are supported: [eq, in_values]")

        if vfilter.eq and vfilter.in_values:
            raise ValueError(f"Only one of operations 'eq' and 'in_values' can be present in the filter for {field_name}")

        if vfilter.eq:
            values = [vfilter.eq]
        else:
            values = vfilter.in_values

        correct_type_operands = all(isinstance(v, str) for v in values)

        if not correct_type_operands:
            raise ValueError(f"Found operand of incorrect type. "
                             f"All operands must be of str type for {field_name}. "
                             f"Operands: {values}")

        kwargs[arg_name] = values

    if 'name' in kwargs and len(kwargs['name']) == 1 and '%' in kwargs['name'][0]:
        kwargs['name'] = kwargs['name'][0]

    return kwargs


def _validate_prepare_args_for_task(task):
    task_type = TaskType(task.task_type)

    targs = task.dict()

    if task_type == TaskType.container:
        task_args = ContainerTaskRunArgs(**targs)
    elif task_type == TaskType.spark:
        task_args = SparkTaskRunArgs(**targs)
    else:
        task_args = TaskRunArgs(**targs)

    return task_args


@app.get("/tasks")
def list_(
        filter: TaskListFilter = Body(
            ...,
            description="filter"
        ),
        include_reason = Query(
            False,
            description="Whatever to include detailed reason or not"
        )
) -> List[tasks.TaskInfoResponse]:
    filter_kwargs = handle_filter(filter)
    logger.debug("Filtering tasks with filter kwargs %s" % filter_kwargs)
    tasks = tasks_manager().list(**filter_kwargs, include_reason=include_reason)
    logger.debug("Obtained all tasks for %s" % filter_kwargs)
    logger.debug("Handled tunings for %s" % filter_kwargs)
    return [convert_task_info_to_response(task) for task in tasks]


@app.get("/task/{uid}")
def get(
    uid: str = Path(
        description="Unique identifier of task to get info about",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    ),
    include_reason = Query(
        False,
        description="Whatever to include detailed reason or not"
    )
) -> TaskInfoResponse:
    task = tasks_manager().get(uid, include_reason=include_reason)
    return convert_task_info_to_response(task)


@app.get("/task/{uid}/result")
def get_result(
    uid: str = Path(
        description="Unique identifier of task to get info about",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    )
) -> TaskResultResponse:
    results = tasks_manager().get_result(uid)
    logger.debug("Results:\n %s" % pprint.pformat(results))
    return TaskResultResponse(uid=uid, results=results)


@app.get("/task/{uid}/log")
def get_log(
    uid: str = Path(
        description="Unique identifier of task to get info about",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    ),
    tail: Optional[int] = Query(
        None,
        description="How many last lines to retrieve from logs",
        example="100"
    )
) -> TaskLogResponse:
    return TaskLogResponse(uid=uid, log=tasks_manager().get_log(uid, tail))


@app.post("/task/start")
async def start(
    task: AvailableTaskTypes = Body(
        ...,
        discriminator='task_type',
        description="Task object containing info about what and how to run"
    )
) -> TaskInfoResponse:
    logger.info("Got task %s" % task)
    task_args = _validate_prepare_args_for_task(task)
    atask = tasks_manager().start(task_args)
    return convert_task_info_to_response(atask.task)


@app.get("/task/{uid}/cancel")
def cancel(
    uid: str = Path(
        description="Unique identifier of task to get info about",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    ),
    wait: Optional[float] = Query(
        None,
        description="If set, forces this method to wait until the task changes its status "
                    "to one of terminal statuses (finished, failed or cancelled). ",
        example="TBD"
    )
) -> TaskInfoResponse:
    tasks_manager().stop(uid)
    task = tasks_manager().get(uid)
    return convert_task_info_to_response(task)
