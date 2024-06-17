import os
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, cast, Optional

import pytest
from rnseism_sdk.db.tasks import TaskStatus
from rnseism_sdk.runner.base import FailedJobException
from rnseism_sdk.worker.utils import TestSDKJob

from geowsm.tasks.batch_tasks import BatchTasksManager, DockerBatchTasksManager, \
    KubernetesBatchTasksManager
from geowsm.tasks.batch_tasks_args import BatchTaskRunArgs, SparkOnK8sBatchTaskRunArgs
from tests.fixtures.db import DBCtx
from tests.utils import check_tasks_status, check_tasks_log, make_ctx, Ctx, only_for_k8s

# commands for debug:
# docker ps -a -f  "label=rn_entity_type=batch-worker" | tail -n +2 | head -n 1 | awk '{print $1}' | xargs docker logs
# docker ps -a -f  "label=rn_entity_type=batch-task" | tail -n +2 | head -n 1 | awk '{print $1}' | xargs docker logs

# 'docker' and/or 'k8s'
RUN_CONTEXT = [el.lower().strip() for el in os.environ.get("GEOWSM_PYTEST_RUN_CONTEXT", "docker").split(',')]


@dataclass
class RunContext:
    manager: BatchTasksManager
    image: str
    ctx: Ctx


@pytest.fixture(scope='function')
def docker_run_context(pytestconfig,
                       auth_token,
                       db_ctx: DBCtx,
                       docker_batch_worker,
                       docker_runner_config,
                       docker_cleaning_iworkers,
                       batch_task_image) -> RunContext:
    return RunContext(
        manager=DockerBatchTasksManager(db_ctx.result_storage),
        image=batch_task_image,
        ctx=make_ctx(pytestconfig, docker_runner_config, auth_token)
    )


@pytest.fixture(scope='function')
def k8s_run_context(pytestconfig,
                    auth_token,
                    k8s_batch_worker,
                    db_ctx: DBCtx,
                    k8s_ctx,
                    k8s_spark_config_map_name,
                    pushable_batch_task_image,
                    pushable_pyspark_executor_image) -> RunContext:
    if k8s_ctx.namespace is None:
        pytest.skip("Kubernetes namespace is not available for test by some reason.")

    return RunContext(
        manager=KubernetesBatchTasksManager(db_ctx.result_storage, k8s_ctx.namespace),
        image=pushable_batch_task_image,
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
def test_batch_task_finished(run_context: RunContext):
    project_id = str(run_context.ctx.project_id)
    user_id = str(run_context.ctx.user_id)
    job_id = TestSDKJob.__name__
    param_result_keys = run_context.ctx.param_kwargs['param_result_keys']

    base_args = BatchTaskRunArgs(
        name="some_test_batch_task",
        user_id=user_id,
        project_id=project_id,
        job_id=job_id,
        parameters={
            'duration': 2,
            'param_result_keys': param_result_keys
        },
        image=run_context.image
    )

    run_args = [deepcopy(base_args) for _ in range(2)]

    # start tasks
    atasks = [run_context.manager.start(run_context.ctx.token, args) for args in run_args]

    # checking all tasks are running
    check_tasks_status(
        run_context.manager,
        atasks=atasks,
        desired_statuses=[TaskStatus.FINISHED, TaskStatus.RUNNING],
        timeout=60
    )

    check_tasks_status(
        run_context.manager,
        atasks=atasks,
        desired_statuses=[TaskStatus.FINISHED],
        timeout=60
    )

    results = [cast(Dict[str, Any], run_context.manager.get_result(atask.task.task_uid)) for atask in atasks]
    for r in results:
        assert set(r.keys()) == {*param_result_keys, 'task_out'}
        del r['task_out']
        assert r == param_result_keys

    check_tasks_log(run_context.manager, atasks, desired_content_part=f'Start of job {job_id}')

    tinfos = [run_context.manager.get(uid=atask.task.task_uid) for atask in atasks]
    assert all(tinfo.end_time is not None for tinfo in tinfos)


@pytest.mark.parametrize('run_context', RUN_CONTEXT, indirect=True)
def test_batch_task_failed(run_context: RunContext):
    project_id = str(run_context.ctx.project_id)
    user_id = str(run_context.ctx.user_id)
    job_id = TestSDKJob.__name__

    param_result_keys = run_context.ctx.param_kwargs['param_result_keys']

    base_args = BatchTaskRunArgs(
        name="some_test_batch_task",
        user_id=user_id,
        project_id=project_id,
        job_id=job_id,
        parameters={
            'duration': 2,
            'param_result_keys': param_result_keys,
            'param_do_fail': True
        },
        image=run_context.image
    )

    run_args = [deepcopy(base_args) for _ in range(2)]

    atasks = [run_context.manager.start(run_context.ctx.token, args) for args in run_args]

    # checking all tasks are failed
    check_tasks_status(
        run_context.manager,
        atasks=atasks,
        desired_statuses=[TaskStatus.FAILED],
        timeout=30
    )

    check_tasks_log(run_context.manager, atasks, desired_content_part=[f'Start of job {job_id}', 'Raising exception'])

    tinfos = [run_context.manager.get(atask.task.task_uid, include_reason=True) for atask in atasks]
    for tinfo in tinfos:
        assert tinfo.reason is not None
        assert FailedJobException.__name__ in tinfo.reason

    assert all(tinfo.end_time is not None for tinfo in tinfos)


@pytest.mark.parametrize('run_context', RUN_CONTEXT, indirect=True)
def test_batch_task_cancelled(run_context: RunContext):
    project_id = str(run_context.ctx.project_id)
    user_id = str(run_context.ctx.user_id)
    job_id = TestSDKJob.__name__

    param_result_keys = {
        'a': [124, 345, 67, 42.0],
        'b': 42.0,
        'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
        'd': False
    }

    base_args = BatchTaskRunArgs(
        name="some_test_batch_task",
        user_id=user_id,
        project_id=project_id,
        job_id=job_id,
        parameters={
            'duration': 10,
            'param_result_keys': param_result_keys
        },
        image=run_context.image
    )

    run_args = [deepcopy(base_args) for _ in range(2)]

    # start tasks
    aresults = [run_context.manager.start(run_context.ctx.token, args) for args in run_args]

    # checking all tasks are running
    check_tasks_status(run_context.manager, atasks=aresults, desired_statuses=TaskStatus.RUNNING, timeout=4)

    stop_task_id = aresults[-1].task.task_uid
    run_context.manager.stop(stop_task_id)

    check_tasks_status(
        run_context.manager,
        atasks=aresults,
        desired_statuses=[TaskStatus.FINISHED, TaskStatus.CANCELLED],
        timeout=60
    )

    statuses = Counter([run_context.manager.get(atask.task.task_uid).status for atask in aresults])
    assert statuses == {TaskStatus.FINISHED: len(run_args) - 1, TaskStatus.CANCELLED: 1}

    tinfos = [run_context.manager.get(atask.task.task_uid) for atask in aresults]
    assert all(tinfo.end_time is not None for tinfo in tinfos)


@pytest.mark.parametrize('run_context', only_for_k8s(RUN_CONTEXT), indirect=True)
def test_batch_spark_task_finished(run_context: RunContext, k8s_spark_config_map_name: Optional[str]):
    project_id = str(run_context.ctx.project_id)
    user_id = str(run_context.ctx.user_id)
    job_id = "SparkTestCustomOpsJob" # "SparkTestJob"

    base_args = BatchTaskRunArgs(
        name="spark_batch_test_task",
        user_id=user_id,
        project_id=project_id,
        job_id=job_id,
        parameters=dict(),
        environment={
            "RNSEISM_SPARK_CLUSTER": "no",
            "RNSEISM_CODEGEN_SPARK_LOCAL_CORES": "4",
            "RNSEISM_CODEGEN_SPARK_LOCAL_MEMORY": "4g",
            "RNSEISM_CODEGEN_STEP_IMPORTER_PATH": "['file:///usr/local/lib/python3.10/site-packages/rnseism_sdk/spark/custom/custom_processors.py']"
        },
        image=run_context.image,
    )

    # start tasks
    atasks = [run_context.manager.start(run_context.ctx.token, base_args)]

    # checking all tasks are running
    check_tasks_status(
        run_context.manager,
        atasks=atasks,
        desired_statuses=[TaskStatus.FINISHED, TaskStatus.RUNNING],
        timeout=60
    )

    check_tasks_status(
        run_context.manager,
        atasks=atasks,
        desired_statuses=[TaskStatus.FINISHED],
        timeout=60
    )

    results = [cast(Dict[str, Any], run_context.manager.get_result(atask.task.task_uid)) for atask in atasks]
    for r in results:
        assert set(r.keys()) == {'task_out'}
        assert 'output_uuids' in r['task_out']
        assert len(r['task_out']['output_uuids']) > 0

    tinfos = [run_context.manager.get(uid=atask.task.task_uid) for atask in atasks]
    assert all(tinfo.end_time is not None for tinfo in tinfos)


@pytest.mark.parametrize('run_context', only_for_k8s(RUN_CONTEXT), indirect=True)
def test_distributed_batch_spark_finished(run_context: RunContext, k8s_spark_config_map_name: Optional[str]):
    project_id = str(run_context.ctx.project_id)
    user_id = str(run_context.ctx.user_id)
    job_id = "SparkTestCustomOpsJob" # "SparkTestJob"

    base_args = SparkOnK8sBatchTaskRunArgs(
        name="spark_batch_test_task",
        user_id=user_id,
        project_id=project_id,
        job_id=job_id,
        parameters=dict(),
        environment={
            "RNSEISM_CODEGEN_STEP_IMPORTER_PATH": "['file:///usr/local/lib/python3.10/site-packages/rnseism_sdk/spark/custom/custom_processors.py']"
        },
        image=run_context.image,
    )

    # start tasks
    atasks = [run_context.manager.start(run_context.ctx.token, base_args)]

    # checking all tasks are running
    check_tasks_status(
        run_context.manager,
        atasks=atasks,
        desired_statuses=[TaskStatus.FINISHED, TaskStatus.RUNNING],
        timeout=60
    )

    check_tasks_status(
        run_context.manager,
        atasks=atasks,
        desired_statuses=[TaskStatus.FINISHED],
        timeout=60
    )

    results = [cast(Dict[str, Any], run_context.manager.get_result(atask.task.task_uid)) for atask in atasks]
    for r in results:
        assert set(r.keys()) == {'task_out'}
        assert 'output_uuids' in r['task_out']
        assert len(r['task_out']['output_uuids']) > 0

    tinfos = [run_context.manager.get(uid=atask.task.task_uid) for atask in atasks]
    assert all(tinfo.end_time is not None for tinfo in tinfos)