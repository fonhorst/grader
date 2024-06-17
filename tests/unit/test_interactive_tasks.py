import logging
import os
import random
import uuid
from collections import Counter
from dataclasses import dataclass
from typing import Dict

import pytest
from rnseism_sdk.db.tasks import TaskStatus
from rnseism_sdk.envs import ENV_VAR_RUNNER_DB_CONN, ENV_VAR_RUNNER_DB_CONN_EXTERNAL
from rnseism_sdk.runner.base import FailedJobException
from rnseism_sdk.session import RedisBasedSessionStatusStorage

from geowsm.tasks.base import TaskResult, TaskInfo
from geowsm.tasks.docker_based_interactive_tasks import DockerBasedInteractiveTasksManager, \
    KubernetesBasedInteractiveTasksManager
from geowsm.tasks.interactive_tasks import InteractiveWorkerStatus, InteractiveTasksManager
from geowsm.tasks.kubernetes.kubernetes_manager import KubernetesManager
from . import check_stored_values
from ..fixtures.db import DBCtx
from ..utils import check_logs, check_tasks_status, check_task_result, make_run_args, wait_for_desired_workers_count, \
    make_ctx, Ctx

logger = logging.getLogger(__name__)


# 'docker' and/or 'k8s'
RUN_CONTEXT = [el.lower().strip() for el in os.environ.get("GEOWSM_PYTEST_RUN_CONTEXT", "docker").split(',')]


@pytest.fixture(scope='function')
def k8s_manager(k8s_ctx, k8s_network_storages) -> KubernetesManager:
    return KubernetesManager(
        network_storages=list(k8s_network_storages.values()),
        namespace=k8s_ctx.namespace
    )


@dataclass
class RunContext:
    manager: InteractiveTasksManager
    image: str
    ctx: Ctx


@pytest.fixture(scope='function')
def docker_run_context(pytestconfig,
                       auth_token,
                       db_ctx,
                       worker_image: str,
                       docker_runner_config,
                       docker_cleaning_iworkers,
                       network_name,
                       ) -> RunContext:
    os.environ[ENV_VAR_RUNNER_DB_CONN] = db_ctx.postgresql_url
    _, worker_config_path = docker_runner_config
    return RunContext(
        manager=DockerBasedInteractiveTasksManager(
            session_status_storage=RedisBasedSessionStatusStorage(db_ctx.session_storage_url),
            result_storage=db_ctx.result_storage,
            celery_broker_url=db_ctx.rabbitmq_url,
            celery_result_backend=f"{db_ctx.redis_url}/1",
            network_name=network_name,
            worker_config_path=worker_config_path
        ),
        image=worker_image,
        ctx=make_ctx(pytestconfig, docker_runner_config, auth_token)
    )


@pytest.fixture(scope='function')
def k8s_run_context(pytestconfig,
                    auth_token,
                    db_ctx,
                    k8s_manager,
                    k8s_ctx,
                    pushable_worker_image) -> RunContext:

    if k8s_ctx.namespace is None:
        pytest.skip("Kubernetes namespace is not available for test by some reason.")

    os.environ[ENV_VAR_RUNNER_DB_CONN] = db_ctx.postgresql_url
    os.environ[ENV_VAR_RUNNER_DB_CONN_EXTERNAL] = db_ctx.external_postgresql_url
    _, _, cm_name = k8s_ctx.external_runner_config

    return RunContext(
        manager=KubernetesBasedInteractiveTasksManager(
            session_status_storage=RedisBasedSessionStatusStorage(db_ctx.session_storage_url),
            result_storage=db_ctx.result_storage,
            celery_broker_url=db_ctx.external_rabbitmq_url,
            celery_result_backend=f"{db_ctx.external_redis_url}/1",
            manager=k8s_manager,
            worker_config_configmap=cm_name
        ),
        image=pushable_worker_image,
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
def test_interactive_task_finished(db_ctx: DBCtx, run_context: RunContext):
    desired_results = {
        'a': [124, 345, 67, 42.0],
        'b': 42.0,
        'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
        'd': False,
        'task_out': {"field_1": "some answer #1", "field_2": 42, "hostname": run_context.ctx.worker_hostname}
    }

    # create worker
    worker_info = run_context.manager.create_worker(
        user_id=str(uuid.uuid4()),
        project_id=str(run_context.ctx.project_id),
        name=run_context.ctx.worker_hostname,
        image=run_context.image,
        hostname=run_context.ctx.worker_hostname
    )
    worker = run_context.manager.get_worker(worker_info.uid)
    assert worker.status == InteractiveWorkerStatus.running

    check_logs(run_context.manager, worker_info.uid, timeout=60)

    # execute task
    awaitable_result = run_context.manager.start(run_context.ctx.token, run_context.ctx.run_args)
    check_tasks_status(
        atasks=[awaitable_result],
        manager=run_context.manager,
        desired_statuses=[TaskStatus.RUNNING],
        timeout=30
    )
    check_task_result(awaitable_result, run_context.ctx.worker_hostname)
    check_tasks_status(
        atasks=[awaitable_result],
        manager=run_context.manager,
        desired_statuses=[TaskStatus.FINISHED],
        timeout=30
    )
    check_stored_values(
        db_ctx.localhost_result_storage_url,
        desired_results,
        prefix=str(awaitable_result.task.task_uid)
    )

    tinfo = run_context.manager.get(awaitable_result.task.task_uid)
    assert tinfo.end_time is not None

    # remove worker and ensure no containers left
    run_context.manager.delete_workers(str(run_context.ctx.project_id))
    wait_for_desired_workers_count(run_context.manager, str(run_context.ctx.project_id), count=0, timeout=10)


@pytest.mark.parametrize('run_context', RUN_CONTEXT, indirect=True)
def test_interactive_task_failed(run_context: RUN_CONTEXT):
    # create worker
    worker_info = run_context.manager.create_worker(
        user_id=str(uuid.uuid4()),
        project_id=str(run_context.ctx.project_id),
        name=run_context.ctx.worker_hostname,
        image=run_context.image,
        hostname=run_context.ctx.worker_hostname
    )
    worker = run_context.manager.get_worker(worker_info.uid)
    assert worker.status == InteractiveWorkerStatus.running

    # execute task
    tar = run_context.manager.start(
        run_context.ctx.token,
        make_run_args(run_args=run_context.ctx.run_args, duration=0.1, do_fail=True)
    )
    check_tasks_status(atasks=[tar], manager=run_context.manager, desired_statuses=[TaskStatus.FAILED], timeout=30)

    tinfos = [run_context.manager.get(tar.task.task_uid, include_reason=True)]
    for tinfo in tinfos:
        assert tinfo.reason is not None
        assert FailedJobException.__name__ in tinfo.reason

    assert all(tinfo.end_time is not None for tinfo in tinfos)


@pytest.mark.parametrize('run_context', RUN_CONTEXT, indirect=True)
def test_interactive_task_cancelled(run_context: RunContext):
    # create worker
    worker_info = run_context.manager.create_worker(
        user_id=str(run_context.ctx.user_id),
        project_id=str(run_context.ctx.project_id),
        name=run_context.ctx.worker_hostname,
        image=run_context.image,
        hostname=run_context.ctx.worker_hostname
    )
    worker = run_context.manager.get_worker(worker_info.uid)
    assert worker.status == InteractiveWorkerStatus.running

    run_args = [
        *(make_run_args(run_args=run_context.ctx.run_args, duration=2) for _ in range(3)),
        make_run_args(run_args=run_context.ctx.run_args, duration=5)
    ]

    atasks = [run_context.manager.start(run_context.ctx.token, args) for args in run_args]

    run_context.manager.stop(atasks[-1].task.task_uid)

    check_tasks_status(
        run_context.manager,
        atasks=atasks,
        desired_statuses=[TaskStatus.FINISHED, TaskStatus.CANCELLED],
        timeout=30
    )
    # time.sleep(10)
    # assert all(r.result.ready() for r in atasks)

    tasks: Dict[str, TaskInfo] = {atask.task.task_uid: run_context.manager.get(atask.task.task_uid) for atask in atasks}
    assert Counter(t.status for t in tasks.values()) == {TaskStatus.FINISHED: 3, TaskStatus.CANCELLED: 1}
    assert tasks[atasks[-1].task.task_uid].status == TaskStatus.CANCELLED

    assert all(tinfo.end_time is not None for tinfo in tasks.values())


@pytest.mark.parametrize('run_context', RUN_CONTEXT, indirect=True)
def test_interactive_tasks_complex(run_context: RunContext):
    worker_hostnames = ["worker-a-1", "worker-a-2", "worker-b-1", "worker-b-2"]

    task_num = 20

    user_id = str(run_context.ctx.user_id)

    # create workers
    winfos = dict()
    for worker_hostname in worker_hostnames:
        worker_info = run_context.manager.create_worker(
            user_id=user_id,
            project_id=str(run_context.ctx.project_id),
            name=worker_hostname,
            image=run_context.image,
            hostname=worker_hostname,
            cpu=1
        )
        worker = run_context.manager.get_worker(worker_info.uid)
        assert worker.status == InteractiveWorkerStatus.running
        winfos[worker.name] = worker

    # check tasks
    atasks = [
        run_context.manager.start(run_context.ctx.token, make_run_args(run_args=run_context.ctx.run_args, duration=0.2))
        for _ in range(task_num)
    ]
    check_tasks_status(run_context.manager, atasks=atasks, desired_statuses=TaskStatus.FINISHED, timeout=80)
    # time.sleep(10)
    # assert all(r.result.ready() for r in atasks)

    counter = Counter([TaskResult.parse_obj(r.result.get()).result['hostname'] for r in atasks])
    logger.info(f"Counter: {counter}")
    assert {wh: counter[wh] >= 0 for wh in worker_hostnames} == {wh: True for wh in worker_hostnames}

    # downscale workers
    worker_hostnames = ["worker-a-2", "worker-b-2"]
    for worker_name in ["worker-a-1", "worker-b-1"]:
        logger.info("Deleting worker %s" % worker_name)
        worker_id = winfos[worker_name].uid
        run_context.manager.delete_worker(worker_id)

    wait_for_desired_workers_count(run_context.manager, str(run_context.ctx.project_id), count=len(worker_hostnames), timeout=30)
    cnts = run_context.manager.list_workers(project_id=str(run_context.ctx.project_id))
    assert {cnt.name for cnt in cnts} == set(worker_hostnames)

    # check tasks
    atasks = [
        run_context.manager.start(run_context.ctx.token, make_run_args(run_args=run_context.ctx.run_args, duration=0.1))
        for _ in range(task_num)
    ]
    check_tasks_status(run_context.manager, atasks=atasks, desired_statuses=TaskStatus.FINISHED, timeout=80)
    # time.sleep(10)
    # assert all(r.result.ready() for r in atasks)

    counter = Counter([TaskResult.parse_obj(r.result.get()).result['hostname'] for r in atasks])
    logger.info(f"Counter: {counter}")
    assert {wh: counter[wh] >= 0 for wh in worker_hostnames} == {wh: True for wh in worker_hostnames}

    # upscale workers back
    worker_hostnames = ["worker-a-1", "worker-a-2", "worker-b-2", "worker-b-1"]
    for worker_name in ["worker-a-1", "worker-b-1"]:
        worker_info = run_context.manager.create_worker(
            user_id=user_id,
            project_id=str(run_context.ctx.project_id),
            name=worker_name,
            image=run_context.image,
            hostname=worker_name,
            cpu=1
        )
        worker = run_context.manager.get_worker(worker_info.uid)
        assert worker.status == InteractiveWorkerStatus.running

    cnts = run_context.manager.list_workers(project_id=str(run_context.ctx.project_id))
    assert {cnt.name for cnt in cnts} == set(worker_hostnames)

    # check tasks
    atasks = [
        run_context.manager.start(run_context.ctx.token, make_run_args(run_args=run_context.ctx.run_args, duration=0.2))
        for _ in range(task_num)
    ]
    check_tasks_status(run_context.manager, atasks=atasks, desired_statuses=TaskStatus.FINISHED, timeout=80)
    # time.sleep(10)
    # assert all(r.result.ready() for r in atasks)

    counter = Counter([TaskResult.parse_obj(r.result.get()).result['hostname'] for r in atasks])
    logger.info(f"Counter: {counter}")
    assert {wh: counter[wh] >= 0 for wh in worker_hostnames} == {wh: True for wh in worker_hostnames}

    run_context.manager.delete_workers(str(run_context.ctx.project_id))
    wait_for_desired_workers_count(run_context.manager, str(run_context.ctx.project_id), count=0)
    cnts = run_context.manager.list_workers(project_id=str(run_context.ctx.project_id))
    assert len(cnts) == 0


@pytest.mark.parametrize('run_context', RUN_CONTEXT, indirect=True)
def test_interactive_tasks_stateful(run_context: RunContext):
    worker_hostnames = ["worker-a-1", "worker-a-2", "worker-a-3"]

    statefull_num_tasks = 20
    stateless_num_tasks = 15

    user_id = str(run_context.ctx.user_id)

    # create workers
    workers = []
    for worker_hostname in worker_hostnames:
        worker_info = run_context.manager.create_worker(
            user_id=user_id,
            project_id=str(run_context.ctx.project_id),
            name=worker_hostname,
            image=run_context.image,
            hostname=worker_hostname,
            cpu=1
        )
        worker = run_context.manager.get_worker(worker_info.uid)
        assert worker.status == InteractiveWorkerStatus.running
        workers.append(worker)

    session_id = str(uuid.uuid4())
    aresult = run_context.manager.start(
        run_context.ctx.token,
        make_run_args(run_args=run_context.ctx.run_args, duration=0.1, session_id=session_id)
    )
    hostname = TaskResult.parse_obj(aresult.result.get()).result['hostname']

    # check tasks
    run_args = [
        *(
            make_run_args(run_args=run_context.ctx.run_args, duration=0.1, session_id=session_id)
            for _ in range(statefull_num_tasks)
        ),
        *(make_run_args(run_args=run_context.ctx.run_args, duration=0.1) for _ in range(stateless_num_tasks))
    ]

    random.shuffle(run_args)

    atasks = [run_context.manager.start(run_context.ctx.token, args) for args in run_args]
    check_tasks_status(run_context.manager, atasks=atasks, desired_statuses=TaskStatus.FINISHED, timeout=60)
    # time.sleep(20)
    # counter = Counter([r.result.ready() for r in atasks])
    # assert all(r.result.ready() for r in atasks)

    counter = Counter([TaskResult.parse_obj(r.result.get()).result['hostname'] for r in atasks])
    logger.info(f"Counter: {counter}")
    # TODO: unstable number of executed tasks. Perhaps related to rabbitmq or celery settings. Or OS scheduling.
    assert counter[hostname] >= statefull_num_tasks
    assert len([wh for wh in worker_hostnames if counter[wh] > 0]) >= 2
