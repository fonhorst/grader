import os
from typing import Dict, Any

import redis
import yaml
from rnseism_sdk.envs import DEFAULT_WORKER_CONFIG_PATH
from rnseism_sdk.sdk.data_storage import RedisDataStorage

from grader.env import ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND, \
    ENV_WMS_CONFIG, ENV_WMS_EXECUTION_BACKEND, ENV_WMS_RESULT_STORAGE_URL
from grader.tasks.base import TasksManager
from grader.tasks.batch_tasks import DockerBatchTasksManager, KubernetesBatchTasksManager
from grader.tasks.kubernetes.kubernetes_manager import KubernetesManager


def _load_config() -> Dict[str, Any]:
    path = os.environ.get(ENV_WMS_CONFIG, None)
    if path and os.path.exists(path):
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
    else:
        config = dict()

    return config


def _create_tasks_manager() -> TasksManager:
    for env_var in [ENV_WMS_EXECUTION_BACKEND, ENV_WMS_RESULT_STORAGE_URL,
                    ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND]:
        if env_var not in os.environ:
            raise ValueError(f"{env_var} env var should be set")

    execution_backend = os.environ[ENV_WMS_EXECUTION_BACKEND]
    result_storage_url = os.environ[ENV_WMS_RESULT_STORAGE_URL]

    config_path = DEFAULT_WORKER_CONFIG_PATH
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    result_storage = RedisDataStorage(prefix="", redis_client=redis.from_url(result_storage_url))
    if execution_backend == 'docker':
        manager = DockerBatchTasksManager(result_storage=result_storage)
    elif execution_backend == 'kubernetes':
        if 'kubernetes_manager' not in config:
            raise ValueError(f"No configuration for 'kubernetes_manager' in {config_path}")

        k8s_manager = KubernetesManager(**config)

        manager = KubernetesBatchTasksManager(result_storage=result_storage, namespace=k8s_manager.namespace)
    else:
        raise ValueError(f"Invalid value for {ENV_WMS_EXECUTION_BACKEND}: {execution_backend}. "
                         f"Only 'kubernetes' and 'docker' are supported.")

    return manager


_tasks_manager = _create_tasks_manager()


def tasks_manager() -> TasksManager:
    return _tasks_manager
