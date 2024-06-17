import os
from copy import copy
from typing import Dict, Any

import redis
import yaml
from rnseism_sdk.envs import DEFAULT_WORKER_CONFIG_PATH

from geowsm.env import ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND, \
    ENV_VAR_BATCH_WORKER_CONFIG_VOLUME
from rnseism_sdk.sdk.data_storage import RedisDataStorage
from rnseism_sdk.session import RedisBasedSessionStatusStorage

from geowsm.tasks.batch_tasks import DockerBatchTasksManager, KubernetesBatchTasksManager
from geowsm.tasks.docker_based_interactive_tasks import DockerBasedInteractiveTasksManager, \
    KubernetesBasedInteractiveTasksManager
from geowsm.tasks.kubernetes.kubernetes_manager import KubernetesManager
from geowsm.tasks.manager import CompositeTasksManager
from geowsm.tasks.nodes import NetworkStorage

from geowsm.env import ENV_WMS_CONFIG, ENV_WMS_SESSION_STATUS_STORAGE_URL, ENV_WMS_RESULT_STORAGE_URL, \
    ENV_WMS_TUNINGS_PROJECTS_AND_USES_API_URL, ENV_WMS_HANDLE_TUNINGS_MODE, ENV_WMS_EXECUTION_BACKEND, CM_NAME, \
    ENV_WMS_CELERY_BROKER_URL_EXTERNAL, ENV_WMS_CELERY_RESULT_BACKEND_EXTERNAL

# the following modes are available: 'normal', 'mock_if_not_found', 'mock'
handle_tunings_mode = os.environ.get(ENV_WMS_HANDLE_TUNINGS_MODE, "normal")
project_and_users_api_url = os.environ[ENV_WMS_TUNINGS_PROJECTS_AND_USES_API_URL] \
    if handle_tunings_mode == 'normal' else None


def _load_config() -> Dict[str, Any]:
    path = os.environ.get(ENV_WMS_CONFIG, None)
    if path and os.path.exists(path):
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
    else:
        config = dict()

    return config


def _create_tasks_manager() -> CompositeTasksManager:
    for env_var in [ENV_WMS_EXECUTION_BACKEND, ENV_WMS_RESULT_STORAGE_URL, ENV_WMS_SESSION_STATUS_STORAGE_URL,
                    ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND]:
        if env_var not in os.environ:
            raise ValueError(f"{env_var} env var should be set")

    execution_backend = os.environ[ENV_WMS_EXECUTION_BACKEND]
    result_storage_url = os.environ[ENV_WMS_RESULT_STORAGE_URL]
    session_storage_url = os.environ[ENV_WMS_SESSION_STATUS_STORAGE_URL]
    celery_broker_url = os.environ.get(ENV_WMS_CELERY_BROKER_URL_EXTERNAL, os.environ[ENV_VAR_CELERY_BROKER_URL])
    celery_result_backend = os.environ.get(ENV_WMS_CELERY_RESULT_BACKEND_EXTERNAL, os.environ[ENV_VAR_CELERY_RESULT_BACKEND])
    worker_config_path = os.environ.get(ENV_VAR_BATCH_WORKER_CONFIG_VOLUME, None)

    config_path = DEFAULT_WORKER_CONFIG_PATH
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    result_storage = RedisDataStorage(prefix="", redis_client=redis.from_url(result_storage_url))
    if execution_backend == 'docker':
        manager = CompositeTasksManager(
            batch_manager=DockerBatchTasksManager(result_storage=result_storage),
            interactive_manager=DockerBasedInteractiveTasksManager(
                session_status_storage=RedisBasedSessionStatusStorage(session_storage_url),
                result_storage=result_storage,
                celery_broker_url=celery_broker_url,
                celery_result_backend=celery_result_backend,
                worker_config_path=worker_config_path
            )
        )
    elif execution_backend == 'kubernetes':
        if 'kubernetes_manager' not in config:
            raise ValueError(f"No configuration for 'kubernetes_manager' in {config_path}")

        cfg = copy(config['kubernetes_manager'])
        network_storages = [NetworkStorage.parse_obj(s) for s in cfg['network_storages']]
        del cfg['network_storages']
        k8s_manager = KubernetesManager(network_storages=network_storages, **cfg)

        manager = CompositeTasksManager(
            batch_manager=KubernetesBatchTasksManager(result_storage=result_storage, namespace=k8s_manager.namespace),
            interactive_manager=KubernetesBasedInteractiveTasksManager(
                session_status_storage=RedisBasedSessionStatusStorage(session_storage_url),
                result_storage=result_storage,
                celery_broker_url=celery_broker_url,
                celery_result_backend=celery_result_backend,
                manager=k8s_manager,
                worker_config_configmap=CM_NAME
            )
        )
    else:
        raise ValueError(f"Invalid value for {ENV_WMS_EXECUTION_BACKEND}: {execution_backend}. "
                         f"Only 'kubernetes' and 'docker' are supported.")

    return manager


# wms_config = _load_config()

_tasks_manager = _create_tasks_manager()


def tasks_manager() -> CompositeTasksManager:
    return _tasks_manager
