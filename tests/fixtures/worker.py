import logging
import os
import tempfile
import time
from typing import Optional

import docker
import pytest
import yaml
from docker.models.containers import Container
from rnseism_sdk.envs import ENV_VAR_RUNNER_DB_CONN, ENV_VAR_RUNNER_DB_CONN_EXTERNAL, DEFAULT_WORKER_CONFIG_PATH

from geowsm.env import ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND, ENV_VAR_BATCH_WORKER_CONFIG_VOLUME, \
    ENV_VAR_WORKER_NETWORK, \
    ENV_VAR_BATCH_WORKER_REMOVE_CONTAINER_POLICY, ENV_VAR_BATCH_WORKER_TYPE
from geowsm.tasks.app import KUBERNETES_BATCH_TASKS_QUEUE, BATCH_TASKS_QUEUE
from geowsm.tasks.base import LABEL_RN_ENTITY_TYPE, RNSEISM_BATCH_TASK, RNSEISM_BATCH_WORKER, RNSEISM_INTERACTIVE_WORKER
from tests.fixtures.db import DBCtx
from tests.utils import remove_containers, log_to_file

logger = logging.getLogger(__name__)


def runner_config(k8s_manager_config, db_ctx: DBCtx):
    logger.info("Preparing runner config")

    worker_config = {
        'jobs_path': '/jobs',
        'jobs': {},
        'parameters_manager': db_ctx.parameters_storage_url,
        'runner': {
            'db_url': db_ctx.postgresql_url,
            'storage_url': db_ctx.storage_url,
            'result_storage_url': db_ctx.result_storage_url,
            'state_storage_url': db_ctx.state_storage_url,
            'session_status_storage_url': db_ctx.session_storage_url

        },
        **({'kubernetes_manager': k8s_manager_config} if k8s_manager_config else dict())
    }

    # env_host_volume = os.getenv("DC_HOST_VOLUME", "/tmp/")
    # env_local_volume_mount = os.getenv("DC_HOST_VOLUME_MOUNT", None)
    p = os.path.join("/tmp", DEFAULT_WORKER_CONFIG_PATH.split('/')[-1])
    with open(p, mode="w+") as wf:
        yaml.dump(worker_config, wf)
        yield worker_config, p


    # if env_host_volume and env_local_volume_mount and os.path.isdir(env_local_volume_mount):
    #     fname = os.path.basename(DEFAULT_WORKER_CONFIG_PATH)  # just file name, no path
    #     local_fpath = os.path.join(env_local_volume_mount, fname)
    #     host_fpath = os.path.join(env_host_volume, fname)
    #
    #     with open(local_fpath, mode="w+") as wf:
    #         yaml.dump(worker_config, wf)
    #     # return path on host to be used by docker daemon on host, if run in devcontainer
    #     yield worker_config, host_fpath
    # else:
    #     with tempfile.NamedTemporaryFile(mode='w+') as fp:
    #         yaml.dump(worker_config, fp)
    #         yield worker_config, fp.name

    logger.info("Runner config has been prepared")


def batch_worker(request,
                 k8s_config,
                 db_ctx: DBCtx,
                 runner_config,
                 worker_image,
                 network_name) -> Optional[Container]:
    logger.info("Starting batch worker container")

    _, config_path = runner_config

    client = docker.from_env()

    # removing batch workers if exist
    remove_containers(
        client=client,
        labels=[f'{LABEL_RN_ENTITY_TYPE}={RNSEISM_BATCH_WORKER}', f'{LABEL_RN_ENTITY_TYPE}={RNSEISM_BATCH_TASK}']
    )

    # creating new batch_worker container
    bw_container: Container = client.containers.run(
        detach=True,
        # network_mode='host',
        network=network_name,
        image=worker_image,
        command=[
            "-A", "geowsm.tasks.batch_worker", "worker",
            "-Q", KUBERNETES_BATCH_TASKS_QUEUE if k8s_config else BATCH_TASKS_QUEUE,
            "--pool=prefork", "--concurrency=2", "--loglevel=DEBUG"
        ],
        hostname=RNSEISM_BATCH_WORKER,
        environment={
            ENV_VAR_CELERY_BROKER_URL: db_ctx.rabbitmq_url,
            ENV_VAR_CELERY_RESULT_BACKEND: f"{db_ctx.redis_url}/1",
            ENV_VAR_WORKER_NETWORK: network_name,
            ENV_VAR_BATCH_WORKER_CONFIG_VOLUME: config_path,
            ENV_VAR_BATCH_WORKER_REMOVE_CONTAINER_POLICY: 'never',
            ENV_VAR_RUNNER_DB_CONN: db_ctx.postgresql_url,
            ENV_VAR_BATCH_WORKER_TYPE: 'kubernetes' if k8s_config else 'docker',
            **(
                {ENV_VAR_RUNNER_DB_CONN_EXTERNAL: db_ctx.external_postgresql_url}  if k8s_config else dict()
            )
        },
        volumes=[
            "/var/run/docker.sock:/var/run/docker.sock",
            f"{config_path}:{DEFAULT_WORKER_CONFIG_PATH}",
            *([f"{k8s_config}:/root/.kube/config"] if k8s_config else [])
        ],
        labels={LABEL_RN_ENTITY_TYPE: RNSEISM_BATCH_WORKER}
    )

    logger.info(f"Started batch worker container {bw_container.id}")

    time.sleep(1)

    bw_container = client.containers.get(bw_container.id)

    assert bw_container.status == 'running'

    yield bw_container

    if request.session.testsfailed:
        fname = f'{RNSEISM_BATCH_WORKER}_{bw_container.id}.log'
        logger.info(f"Some of the tests failed. Writing batch worker logs to {fname}")
        
        log_to_file(bw_container.logs(), fname, dir='session')

    logger.info(f"Stopping and removing batch worker container {bw_container.id}")

    bw_container.stop()
    bw_container.remove()

    logger.info(f"Stopped and removed batch worker containr {bw_container.id}")

    remove_containers(client=client, labels=[f'{LABEL_RN_ENTITY_TYPE}={RNSEISM_BATCH_TASK}'])

    logger.info("Stopped and removed task containers if they existed")


@pytest.fixture(scope='session')
def docker_runner_config(db_ctx: DBCtx):
    yield from runner_config(None, db_ctx)


@pytest.fixture(scope='session')
def docker_batch_worker(request,
                        db_ctx,
                        docker_runner_config,
                        worker_image,
                        network_name):
    yield from batch_worker(
        request,
        None,
        db_ctx,
        docker_runner_config,
        worker_image,
        network_name
    )


@pytest.fixture(scope='function')
def docker_cleaning_iworkers():
    client = docker.from_env()

    logger.info("Cleaning interactive workers")
    remove_containers(
        client=client,
        labels=[f'{LABEL_RN_ENTITY_TYPE}={RNSEISM_INTERACTIVE_WORKER}']
    )

    yield

    logger.info("Cleaning interactive workers")
    remove_containers(
        client=client,
        labels=[f'{LABEL_RN_ENTITY_TYPE}={RNSEISM_INTERACTIVE_WORKER =}']
    )


@pytest.fixture(scope='session')
def k8s_runner_config(k8s_ctx, db_ctx: DBCtx):
    yield from runner_config(k8s_ctx.manager_config, db_ctx)


@pytest.fixture(scope='session')
def k8s_batch_worker(request,
                     k8s_ctx,
                     db_ctx,
                     k8s_runner_config,
                     pushable_worker_image,
                     network_name):
    yield from batch_worker(
        request,
        k8s_ctx.config,
        db_ctx,
        # ? k8s_ctx.external_runner_config
        k8s_runner_config,
        pushable_worker_image,
        network_name
    )
