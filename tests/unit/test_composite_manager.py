import datetime
import os
import random
import uuid
from dataclasses import dataclass

import pytest
from rnseism.models.base import Navigation, OrderBy
from rnseism_sdk.db.tasks import TaskStatus, TaskType
from rnseism_sdk.envs import ENV_VAR_RUNNER_DB_CONN, ENV_VAR_RUNNER_DB_CONN_EXTERNAL
from rnseism_sdk.session import RedisBasedSessionStatusStorage
from rnseism_sdk.worker.utils import TestSDKJob

from geowsm.tasks.base import split_complex_uid
from geowsm.tasks.batch_tasks import DockerBatchTasksManager, KubernetesBatchTasksManager
from geowsm.tasks.batch_tasks_args import BatchTaskRunArgs
from geowsm.tasks.docker_based_interactive_tasks import DockerBasedInteractiveTasksManager, \
    KubernetesBasedInteractiveTasksManager
from geowsm.tasks.interactive_tasks import InteractiveWorkerStatus, InteractiveTaskRunArgs
from geowsm.tasks.kubernetes.kubernetes_manager import KubernetesManager
from geowsm.tasks.manager import CompositeTasksManager
from tests.fixtures.db import DBCtx
from tests.utils import check_tasks_status, check_tasks_log, make_run_args, Ctx, make_ctx

RUN_CONTEXT =  [el.lower().strip() for el in os.environ.get("GEOWSM_PYTEST_RUN_CONTEXT", "docker").split(',')]


@pytest.fixture(scope='function')
def k8s_manager(k8s_ctx, k8s_network_storages) -> KubernetesManager:
    return KubernetesManager(
        network_storages=list(k8s_network_storages.values()),
        namespace=k8s_ctx.namespace
    )


@dataclass
class RunContext:
    manager: CompositeTasksManager
    iworker_image: str
    batch_task_image: str
    ctx: Ctx


@pytest.fixture(scope='function')
def docker_run_context(pytestconfig,
                       auth_token,
                       db_ctx: DBCtx,
                       network_name,
                       docker_runner_config,
                       docker_batch_worker,
                       docker_cleaning_iworkers,
                       worker_image,
                       batch_task_image) -> RunContext:
    os.environ[ENV_VAR_RUNNER_DB_CONN] = db_ctx.postgresql_url
    _, worker_config_path = docker_runner_config
    return RunContext(
        manager=CompositeTasksManager(
            batch_manager=DockerBatchTasksManager(db_ctx.result_storage),
            interactive_manager=DockerBasedInteractiveTasksManager(
                session_status_storage=RedisBasedSessionStatusStorage(db_ctx.session_storage_url),
                result_storage=db_ctx.result_storage,
                celery_broker_url=db_ctx.rabbitmq_url,
                celery_result_backend=f"{db_ctx.redis_url}/1",
                network_name=network_name,
                worker_config_path=worker_config_path
            )
        ),
        iworker_image=worker_image,
        batch_task_image=batch_task_image,
        ctx=make_ctx(pytestconfig, docker_runner_config, auth_token)
    )


@pytest.fixture(scope='function')
def k8s_run_context(pytestconfig,
                    auth_token,
                    db_ctx,
                    k8s_ctx,
                    k8s_batch_worker,
                    k8s_manager,
                    pushable_worker_image,
                    pushable_batch_task_image) -> RunContext:

    if k8s_ctx.namespace is None:
        pytest.skip("Kubernetes namespace is not available for test by some reason.")

    os.environ[ENV_VAR_RUNNER_DB_CONN] = db_ctx.postgresql_url
    os.environ[ENV_VAR_RUNNER_DB_CONN_EXTERNAL] = db_ctx.external_postgresql_url
    _, _, cm_name = k8s_ctx.external_runner_config

    return RunContext(
        manager=CompositeTasksManager(
            batch_manager=KubernetesBatchTasksManager(db_ctx.result_storage, k8s_ctx.namespace),
            interactive_manager=KubernetesBasedInteractiveTasksManager(
                session_status_storage=RedisBasedSessionStatusStorage(db_ctx.session_storage_url),
                result_storage=db_ctx.result_storage,
                celery_broker_url=db_ctx.external_rabbitmq_url,
                celery_result_backend=f"{db_ctx.external_redis_url}/1",
                manager=k8s_manager,
                worker_config_configmap=cm_name
            )
        ),
        iworker_image=pushable_worker_image,
        batch_task_image=pushable_batch_task_image,
        ctx=make_ctx(pytestconfig, k8s_ctx.external_runner_config, auth_token)
    )


@pytest.fixture(scope='function')
def run_context(request) -> RunContext:
    if request.param == 'docker':
        return request.getfixturevalue('docker_run_context')

    if request.param == 'k8s':
        return request.getfixturevalue('k8s_run_context')

    raise ValueError(f'Unsupported manager type: {request.param}')


@pytest.mark.parametrize('run_context', RUN_CONTEXT, indirect=True)
def test_composite_manager(run_context: RunContext):
    # create interactive worker
    worker_hostname = "worker-1"
    worker_info = run_context.manager.interactive_manager.create_worker(
        user_id=str(uuid.uuid4()),
        project_id=str(run_context.ctx.project_id),
        name=worker_hostname,
        image=run_context.iworker_image,
        hostname=worker_hostname,
        cpu=1
    )
    worker = run_context.manager.interactive_manager.get_worker(worker_info.uid)
    assert worker.status == InteractiveWorkerStatus.running

    # start taks
    batch_args = BatchTaskRunArgs(
        name="some_test_batch_task",
        user_id=str(uuid.uuid4()),
        project_id=str(uuid.uuid4()),
        job_id=TestSDKJob.__name__,
        parameters={
            'duration': 2,
            'param_result_keys':  {
                'a': [124, 345, 67, 42.0],
                'b': 42.0,
                'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
                'd': False
            }
        },
        image=run_context.batch_task_image
    )
    interactive_awaitable_result = run_context.manager.start(run_context.ctx.token, run_context.ctx.run_args)
    batch_awaitable_result = run_context.manager.start(run_context.ctx.token, batch_args)

    # wait for tasks to complete
    timeout = 30
    interactive_awaitable_result.result.get(timeout=timeout)
    try:
        batch_awaitable_result.result.get(timeout=timeout)
    except Exception as ex:
        raise ex

    # checking listing
    check_tasks_status(
        run_context.manager,
        atasks=[interactive_awaitable_result, batch_awaitable_result],
        desired_statuses=TaskStatus.FINISHED
    )

    # checking interactive task
    task_info = run_context.manager.get(uid=interactive_awaitable_result.task.uid)
    assert task_info.status == TaskStatus.FINISHED

    task_result = run_context.manager.get_result(uid=interactive_awaitable_result.task.uid)
    assert task_result['task_out'] == {'field_1': 'some answer #1', 'field_2': 42, 'hostname': worker_hostname}

    # checking batch task
    task_info = run_context.manager.get(uid=batch_awaitable_result.task.uid)
    assert task_info.status == TaskStatus.FINISHED

    check_tasks_log(run_context.manager, atasks=[batch_awaitable_result])

    # TODO: add checking results later. Require to change docker image of the task.
    run_context.manager.get_result(uid=batch_awaitable_result.task.uid)

    # checking cancelling
    atasks = [
        run_context.manager.start(run_context.ctx.token, make_run_args(run_args=run_context.ctx.run_args, duration=5)),
        run_context.manager.start(run_context.ctx.token, batch_args)
    ]
    check_tasks_status(run_context.manager, atasks=atasks, desired_statuses=TaskStatus.RUNNING, timeout=2.0)

    for atask in atasks:
        run_context.manager.stop(atask.task.uid)

    check_tasks_status(run_context.manager, atasks=atasks, desired_statuses=TaskStatus.CANCELLED, timeout=5.0)


@pytest.mark.parametrize('run_context', RUN_CONTEXT, indirect=True)
def test_composite_manager_filters_and_navigation(run_context: RunContext):
    ###############Preparing infrastructure
    num_projects, num_users = 3, 2
    user_ids = [str(uuid.uuid4()) for _ in range(num_users)]
    project_ids = [str(uuid.uuid4()) for _ in range(num_projects)]

    btask_args = [(project_ids[i], user_ids[i]) for i in range(2)]

    itask_args = [
        *((pid, uid, False) for pid in project_ids for uid in user_ids),
        *((project_ids[i], user_ids[i], True) for i in range(2))
    ]

    # create interactive worker
    for i, project_id in enumerate(project_ids):
        worker_hostname = f"worker-{i}"
        worker_info = run_context.manager.interactive_manager.create_worker(
            user_id=str(uuid.uuid4()),
            project_id=project_id,
            name=worker_hostname,
            image=run_context.iworker_image,
            hostname=worker_hostname,
            cpu=1
        )
        worker = run_context.manager.interactive_manager.get_worker(worker_info.uid)
        assert worker.status == InteractiveWorkerStatus.running

    ###############Preparing tasks
    itasks = [
        InteractiveTaskRunArgs(
            name=f"some_test_interactive_task_{i}",
            user_id=targs[1],
            project_id=targs[0],
            job_id=TestSDKJob.__name__,
            parameters={
                'duration': 1,
                'param_do_fail': targs[2],
                'param_result_keys': {
                    'a': [124, 345, 67, 42.0],
                    'b': 42.0,
                    'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
                    'd': False,
                }
            }
        ) for i, targs in enumerate(itask_args)
    ]

    btasks = [
        BatchTaskRunArgs(
            name=f"some_test_batch_task_{i}",
            user_id=targs[1],
            project_id=targs[0],
            job_id=TestSDKJob.__name__,
            parameters={
                'duration': 1,
                'param_result_keys':  {
                    'a': [124, 345, 67, 42.0],
                    'b': 42.0,
                    'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
                    'd': False
                }
            },
            image=run_context.batch_task_image
        ) for i, targs in enumerate(btask_args)
    ]

    tasks = [*itasks, *btasks]
    random.shuffle(tasks)

    first_part_tasks, second_part_tasks = tasks[:2], tasks[2:]

    ###############Starting tasks
    first_part_begin = datetime.datetime.now()
    first_part_atasks = [run_context.manager.start(run_context.ctx.token, task) for task in first_part_tasks]
    check_tasks_status(
        run_context.manager,
        atasks=first_part_atasks,
        desired_statuses=[TaskStatus.FINISHED, TaskStatus.FAILED],
        timeout=60.0
    )

    second_part_begin = datetime.datetime.now()
    second_part_atasks = [run_context.manager.start(run_context.ctx.token, task) for task in second_part_tasks]
    check_tasks_status(
        run_context.manager,
        atasks=second_part_atasks,
        desired_statuses=[TaskStatus.FINISHED, TaskStatus.FAILED],
        timeout=60.0
    )

    atasks = [*first_part_atasks, *second_part_atasks]

    ###############Checking tasks
    tinfos = run_context.manager.list(project_ids=[project_ids[0]])
    assert len(tinfos) == num_users + 2

    tinfos = run_context.manager.list(project_ids=[project_ids[2]])
    assert len(tinfos) == num_users

    tinfos = run_context.manager.list(project_ids=project_ids)
    assert len(tinfos) == num_users * num_projects + 4

    tinfos = run_context.manager.list(
        project_ids=project_ids,
        navigation=Navigation(offset=2, limit=4, order=[OrderBy(by='user_id'), OrderBy(by='project_id')])
    )
    assert len(tinfos) == 4

    tinfos = run_context.manager.list(project_ids=[project_ids[0]], author_ids=[user_ids[0]])
    assert len(tinfos) == 3

    tinfos = run_context.manager.list(project_ids=project_ids, author_ids=user_ids, statusess=[TaskStatus.FINISHED.value])
    assert len(tinfos) == 8

    tinfos = run_context.manager.list(project_ids=project_ids, author_ids=user_ids, statusess=[TaskStatus.FAILED.value])
    assert len(tinfos) == 2

    tinfos = run_context.manager.list(project_ids=project_ids, author_ids=user_ids, task_types=[TaskType.interactive.value])
    assert len(tinfos) == len(itask_args)

    tinfos = run_context.manager.list(project_ids=project_ids, author_ids=user_ids, task_types=[TaskType.batch.value])
    assert len(tinfos) == len(btask_args)

    # checking by uuids
    b_tasks = [atask for atask in atasks if atask.task.task_type == TaskType.batch]
    i_tasks = [atask for atask in atasks if atask.task.task_type == TaskType.interactive]
    m_tasks = [i_tasks[0], b_tasks[0]]

    b_tinfos = run_context.manager.list(uuids=[split_complex_uid(atask.task.uid)[1] for atask in b_tasks])
    i_tinfos = run_context.manager.list(uuids=[split_complex_uid(atask.task.uid)[1] for atask in i_tasks])
    m_tinfos = run_context.manager.list(uuids=[split_complex_uid(atask.task.uid)[1] for atask in m_tasks])

    assert len(b_tinfos) == len(b_tasks)
    assert len(i_tinfos) == len(i_tasks)
    assert len(m_tinfos) == len(m_tasks)

    # checking by submit_time
    tinfos = run_context.manager.list(submit_time=(first_part_begin.timestamp(), None))
    assert len(tinfos) == len(atasks)

    tinfos = run_context.manager.list(submit_time=(second_part_begin.timestamp(), None))
    assert len(tinfos) == len(second_part_atasks)

    # checking by name
    tinfos = run_context.manager.list(name='%_test_batch_task_%')
    assert len(tinfos) == len(btasks)

    tinfos = run_context.manager.list(name='%_test_interactive_task_%')
    assert len(tinfos) == len(itasks)

    tinfos = run_context.manager.list(name='%_0')
    assert len(tinfos) == 2

    tinfos = run_context.manager.list(name='some_test_batch_task_0')
    assert len(tinfos) == 1

    tinfos = run_context.manager.list(name=['some_test_batch_task_0', 'some_test_interactive_task_0'])
    assert len(tinfos) == 2
