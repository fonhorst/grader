import logging
import os
import shutil
import time

import docker
import pytest
from urllib.parse import urljoin
from docker.models.containers import Container
from rnseism_sdk.envs import ENV_VAR_RUNNER_DB_CONN, ENV_VAR_RUNNER_DB_CONN_EXTERNAL, DEFAULT_WORKER_CONFIG_PATH

from geowsm.env import ENV_WMS_RESULT_STORAGE_URL, ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND, \
    ENV_WMS_SESSION_STATUS_STORAGE_URL, ENV_VAR_BATCH_WORKER_CONFIG_VOLUME, ENV_VAR_WORKER_NETWORK, \
    ENV_WMS_HANDLE_TUNINGS_MODE, ENV_WMS_TUNINGS_PROJECTS_AND_USES_API_URL, ENV_WMS_EXECUTION_BACKEND, \
    ENV_WMS_CELERY_BROKER_URL_EXTERNAL, ENV_WMS_CELERY_RESULT_BACKEND_EXTERNAL
from geowsm.tasks.base import LABEL_RN_ENTITY_TYPE, RNSEISM_INTERACTIVE_WORKER, RNSEISM_BATCH_TASK
from tests.utils import AppContext, extract_user_and_project, remove_containers, get_session, log_to_file

logger = logging.getLogger(__name__)

EXTERNAL_API_URL = 'http://10.32.0.243'
USE_MOCK_FOR_USER_INFO = True


def app_context(request,
                pytestconfig,
                auth_token,
                main_app_image,
                k8s_config,
                runner_config,
                worker_image,
                batch_task_image,
                network_name,
                db_ctx) -> AppContext:
    """Wait for the api from my_api_service to become responsive"""
    logger.info("Starting main app container")

    # constants
    api_port = "5000"
    mapped_port = "5000"
    api_host = os.getenv("DC_API_CONTAINER_NAME", "localhost")
    # TODO: use api_port or mapped_port in api_url under corresponding conditions
    api_url = f"http://{api_host}:{mapped_port}"
    ping_url = urljoin(api_url, "docs#/")
    ports_mapping = {'ports': {api_port: mapped_port}}  # ports are in the reverse order opposed to docker run -p

    host_root_path = os.getenv("DC_HOST_PROJECT_ROOT", str(pytestconfig.rootpath))
    jwt_private_key_path = os.path.join(host_root_path, "deployment/rsa_test_keys/jwtRS256.key")
    jwt_private_key_container_path = "/jwt_key"
    # extract user_id and project_id
    os.environ["JWT_PRIVATE_KEY_PATH"] = jwt_private_key_path

    user_id, project_id = extract_user_and_project(pytestconfig, auth_token)
    _, runner_config_path = runner_config
    ht_mode, projects_users_api_url = (
        ('mock', '') if USE_MOCK_FOR_USER_INFO else ('normal', EXTERNAL_API_URL)
    )

    container_name = os.getenv("DC_API_CONTAINER_NAME", "geowsm-api")

    client = docker.from_env()

    logger.info("Stopping and removing all cotainers possibly left from previous runs")

    # removing batch workers if exist
    remove_containers(client=client, labels=['type=main_app', f'{LABEL_RN_ENTITY_TYPE}={RNSEISM_INTERACTIVE_WORKER}'])

    # creating new batch_worker container
    main_app_container: Container = client.containers.run(
        name=container_name,
        image=main_app_image,
        detach=True,
        environment={
            "JWT_PRIVATE_KEY_PATH": jwt_private_key_container_path,
            ENV_VAR_CELERY_BROKER_URL: f"{db_ctx.rabbitmq_url}",
            ENV_VAR_CELERY_RESULT_BACKEND: f"{db_ctx.redis_url}/1",
            ENV_WMS_RESULT_STORAGE_URL: f"{db_ctx.redis_url}/2",
            ENV_WMS_SESSION_STATUS_STORAGE_URL: f"{db_ctx.redis_url}/4",
            ENV_VAR_BATCH_WORKER_CONFIG_VOLUME: runner_config_path,
            ENV_VAR_WORKER_NETWORK: network_name,
            ENV_WMS_HANDLE_TUNINGS_MODE: ht_mode,
            ENV_WMS_TUNINGS_PROJECTS_AND_USES_API_URL: projects_users_api_url,
            ENV_VAR_RUNNER_DB_CONN: db_ctx.postgresql_url,
            ENV_WMS_EXECUTION_BACKEND: 'kubernetes' if k8s_config else 'docker',
            **(
                {
                    ENV_VAR_RUNNER_DB_CONN_EXTERNAL: db_ctx.external_postgresql_url,
                    ENV_WMS_CELERY_BROKER_URL_EXTERNAL: f"{db_ctx.external_rabbitmq_url}",
                    ENV_WMS_CELERY_RESULT_BACKEND_EXTERNAL: f"{db_ctx.external_redis_url}/1"
                } if k8s_config else dict()
            )
        },
        volumes=[
            "/var/run/docker.sock:/var/run/docker.sock",
            f"{jwt_private_key_path}:{jwt_private_key_container_path}",
            f"{runner_config_path}:{DEFAULT_WORKER_CONFIG_PATH}",
            *([f"{k8s_config}:/root/.kube/config"] if k8s_config else [])
        ],
        labels={'type': 'main_app'},
        **(
            {'network_mode': 'host'}
            # ports_mapping
            if network_name == 'host'
            else dict(ports_mapping, network=network_name, hostname='main_app')
        )
    )

    logger.info(f"Started main app container {main_app_container.id}")

    time.sleep(1)

    main_app_container = client.containers.get(main_app_container.id)

    assert main_app_container.status == 'running'

    request_session = get_session()
    assert request_session.get(ping_url)
    assert auth_token

    yield AppContext(
        session=request_session,
        api_url=api_url,
        token=auth_token,
        user_id=user_id,
        project_id=project_id,
        worker_image=worker_image,
        batch_task_image=batch_task_image
    )

    if request.session.testsfailed:
        fname = f'main-app_{main_app_container.id}.log'
        logger.info(f"Some of the tests failed. Writing main app logs to {fname}")
        log_to_file(main_app_container.logs(), fname, dir='session')

    logger.info("Stopping and removing all dependant containers if they exist")

    # removing batch workers if exist
    remove_containers(client=client, labels=['type=interactive_worker'])

    logger.info(f"Stopping and removing main app container {main_app_container.id}")

    main_app_container.stop()
    # main_app_container.remove()

    logger.info(f"Stopped and removed main app containr {main_app_container.id}")


@pytest.fixture(scope='session')
def docker_app_context(request,
                       pytestconfig,
                       auth_token,
                       main_app_image,
                       docker_batch_worker,
                       worker_image,
                       batch_task_image,
                       docker_runner_config,
                       network_name,
                       db_ctx):
    yield from app_context(
        request,
        pytestconfig,
        auth_token,
        main_app_image,
        None,
        docker_runner_config,
        worker_image,
        batch_task_image,
        network_name,
        db_ctx
    )


@pytest.fixture(scope='session')
def k8s_app_context(request,
                    pytestconfig,
                    k8s_ctx,
                    auth_token,
                    main_app_image,
                    k8s_batch_worker,
                    pushable_worker_image,
                    pushable_batch_task_image,
                    k8s_runner_config,
                    network_name,
                    db_ctx):
    yield from app_context(
        request,
        pytestconfig,
        auth_token,
        main_app_image,
        k8s_ctx.config,
        k8s_runner_config,
        pushable_worker_image,
        pushable_batch_task_image,
        network_name,
        db_ctx
    )


@pytest.fixture(scope='session')
def remote_app_context(pytestconfig, auth_token, pushable_worker_image, pushable_batch_task_image) -> AppContext:
    user_id, project_id = extract_user_and_project(pytestconfig, auth_token)
    request_session = get_session()
    assert request_session.get(EXTERNAL_API_URL + "/docs#/")
    assert auth_token

    yield AppContext(
        session=request_session,
        api_url=EXTERNAL_API_URL,
        token=auth_token,
        user_id=user_id,
        project_id=project_id,
        worker_image=pushable_worker_image,
        batch_task_image=pushable_batch_task_image
    )


@pytest.fixture(scope="session", autouse=True)
def delete_log_directory(request):
    log_directory = os.path.join(request.config.rootpath, 'tests', 'logs')
    if os.path.exists(log_directory):
        shutil.rmtree(log_directory)


@pytest.fixture(scope="function", autouse=True)
def log_retriever(request):
    client = docker.from_env()
    label_entity_types = [RNSEISM_INTERACTIVE_WORKER, RNSEISM_BATCH_TASK]

    num_tests = request.session.testsfailed

    cts1 = client.containers.list(all=True)
    cts1 = [ctn for ctn in cts1 if ctn.labels.get(LABEL_RN_ENTITY_TYPE) in label_entity_types]
    ctn_ids1 = {ctn.id for ctn in cts1}

    yield

    test_failed = bool(request.session.testsfailed - num_tests)

    if not test_failed:
        return

    cts2 = client.containers.list(all=True)
    cts2 = [ctn for ctn in cts2 if ctn.labels.get(LABEL_RN_ENTITY_TYPE) in label_entity_types]
    ctn_ids2 = {ctn.id for ctn in cts2}
    diff = ctn_ids2 - ctn_ids1

    cts_to_log = [ctn for ctn in cts2 if ctn.id in diff]

    for ctn in cts_to_log:
        ctn_label = ctn.labels.get(LABEL_RN_ENTITY_TYPE)
        fname = f'{ctn_label}_{ctn.id}.log'
        logger.info(f"Worker-generating test failed. Writing {ctn_label} log to {fname}")
        log_to_file(ctn.logs(), filename=fname, dir=f'workers')
