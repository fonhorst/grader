import datetime
import logging
import pprint
from typing import Optional, List, Dict, Any, Union
from urllib.parse import urlparse, ParseResult

from fastapi import Depends, Body, Path, Query
from rnseism.models.base import Navigation

from geowsm.api.endpoints.utils import handle_source
from geowsm.api.services.codegen_introspect_service import CodegenIntrospectionService
from rnseism_sdk.db.tasks import TaskType

from geowsm.api.exceptions.common import JsonrpcError, NO_IWORKERS
from geowsm.tasks.exceptions import NoInteractiveWorkersError
from geowsm.tasks.base import TaskInfo, split_complex_uid
from geowsm.tasks.batch_tasks_args import BatchTaskRunArgs, SparkOnK8sBatchTaskRunArgs
from geowsm.tasks.interactive_tasks import InteractiveTaskRunArgs

from geowsm.api.base import tasks_manager
from geowsm.api.endpoints.utils import TokenInfo, get_token, check_project_exists, check_user_exists, handle_tunings, \
    batch_handle_tunings, parse_job_id
from geowsm.api.schemas import tasks
from geowsm.api.schemas.tasks import TaskListFilter, TaskTunings, TaskInfoResponse, TaskResultResponse, TaskLogResponse, \
    ComputableFunction, \
    AsyncCapableTasks, \
    SyncCapableTasks, TaskStatus, TaskEvent, NodeInfo
from geowsm.app import TASKS_MANAGEMENT_SECTION, jsonrpc_api_v1, app

METHOD_PREFIX = "Task"


logger = logging.getLogger(__name__)


def format_datetime(dt: Optional[Union[float, datetime.datetime]]) -> Optional[str]:
    if not dt:
        return None

    if isinstance(dt, float):
        return datetime.datetime.fromtimestamp(dt).isoformat()

    return dt.isoformat()


def convert_task_info_to_response(
        task: TaskInfo,
        author_name: Optional[str] = None,
        project_name: Optional[str] = None,
        project_info: Optional[str] = None,
        nodes: Optional[List[NodeInfo]] = None,
        events: Optional[List[TaskEvent]] = None) -> TaskInfoResponse:
    task_response = TaskInfoResponse(
        **task.dict(exclude={'uid', 'task_type', 'status', 'status_updated_at', 'submit_time', 'end_time'}),
        uuid=task.uid,
        submit_time=format_datetime(task.submit_time),
        end_time=format_datetime(task.end_time),
        task_type=task.task_type.value,
        status=TaskStatus(task.status.value),
        author_name=author_name,
        project_name=project_name,
        project_info=project_info,
        nodes=nodes,
        events=events
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
        ('uuid', 'uuids', filter.uuid),
        ('name', 'name', filter.name),
        ('project_id', 'project_ids', filter.project_uid),
        ('user_uid', 'author_ids', filter.user_uid),
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

    if 'uuids' in kwargs:
        kwargs['uuids'] = [split_complex_uid(uid)[1] for uid in kwargs['uuids']]

    if 'name' in kwargs and len(kwargs['name']) == 1 and '%' in kwargs['name'][0]:
        kwargs['name'] = kwargs['name'][0]

    return kwargs


async def _validate_prepare_args_for_task(codegen_service, task, token):
    supported_task_types = [TaskType.batch, TaskType.interactive, TaskType.spark]

    task_type = TaskType(task.task_type)
    if task_type not in supported_task_types:
        raise ValueError(f'Unsupported task type "{task.task_type}". '
                         f'Only the following types are supported: {supported_task_types}')

    check_project_exists(token.project_id)
    check_user_exists(token.user_id)

    await handle_source(codegen_service, task, task_type, token)

    targs = {
        'user_id': token.user_id,
        'project_id': token.project_id,
        **task.dict(exclude={'task_type', 'job_source'})
    }

    if task_type == TaskType.batch:
        task_args = BatchTaskRunArgs(**targs)
    elif task_type == TaskType.spark:
        task_args = SparkOnK8sBatchTaskRunArgs(**targs)
    else:
        task_args = InteractiveTaskRunArgs(**targs, session_id=task.session_uid)

    return task_args


@app.get("/tasks")
def list_(
    filter: TaskListFilter = Body(..., description="filter"),
    tunings: TaskTunings = Body(..., description="tunings"),
    navigation: Navigation = Body(..., description="navigation")
) -> List[tasks.TaskInfoResponse]:
    filter_kwargs = handle_filter(filter)
    logger.debug("Filtering tasks with filter kwargs %s" % filter_kwargs)
    tasks = tasks_manager().list(**filter_kwargs, navigation=navigation,
                                 include_reason=(tunings and tunings.include_reason))
    logger.debug("Obtained all tasks for %s" % filter_kwargs)
    uuid2kwargs = batch_handle_tunings(tasks, tunings)
    logger.debug("Handled tunings for %s" % filter_kwargs)
    return [convert_task_info_to_response(task, **uuid2kwargs[task.uid]) for task in tasks]


@app.get("/task/{uid}")
def get(
    uid: str = Path(
        description="Unique identifier of task to get info about",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    ),
    tunings: Optional[TaskTunings] = Body(None, description="tunings")
) -> TaskInfoResponse:
    task = tasks_manager().get(uid, include_reason=(tunings and tunings.include_reason))
    kwargs = handle_tunings(task, tunings)
    return convert_task_info_to_response(task, **kwargs)


@app.get("/task/{uid}/result")
def get_result(
    uid: str = Path(
        description="Unique identifier of task to get info about",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    )
) -> TaskResultResponse:
    results = tasks_manager().get_result(uid)
    # TODO: set correct level here
    logger.warning("Results:\n %s" % pprint.pformat(results))
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


@app.get("/task/start")
async def start(
    task: AsyncCapableTasks = Body(
        ...,
        discriminator='task_type',
        description="Task object containing info about what and how to run",
        example="TBD"
    )
) -> TaskInfoResponse:
    logger.info("Got task %s" % task)

    task_args = await _validate_prepare_args_for_task(codegen_service, task, token)

    try:
        atask = tasks_manager().start(token.token, task_args)
    except NoInteractiveWorkersError as e:
        raise JsonrpcError(code=NO_IWORKERS, message=str(e))

    return convert_task_info_to_response(atask.task)


@app.get("/task/{uid}/cancel")
def cancel(
    uid: str = Path(
        description="Unique identifier of task to get info about",
        example="b6da673d-116f-4177-8cc6-34e101cb0b17"
    ),
    wait: Optional[float] = Query(
        None,
        description="If set, forces this method to wait until the task is changes its status "
                    "to one of terminal statuses (finished, failed or cancelled). "
                    "If set to 0 or -1, the method will wait until the status changes to a terminal status. "
                    "If set to a positive value, the method will wait until either the status changes "
                    "to a terminal status or the timeout is exceeded.",
        example="TBD"
    )
) -> TaskInfoResponse:
    # TODO: Need to add sync version of the method
    tasks_manager().stop(uid)
    task = tasks_manager().get(uid)
    return convert_task_info_to_response(task)
