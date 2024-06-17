import os
from typing import Dict, Any

import redis
import yaml

from grader.env import ENV_VAR_CELERY_BROKER_URL, ENV_VAR_CELERY_RESULT_BACKEND, ENV_VAR_CONFIG, \
    ENV_VAR_EXECUTION_BACKEND, ENV_VAR_RESULT_STORAGE_URL, DEFAULT_WORKER_CONFIG_PATH
from grader.tasks.base import TasksManager
from grader.tasks.batch_tasks import DockerBatchTasksManager, KubernetesBatchTasksManager
from grader.tasks.kubernetes.kubernetes_manager import KubernetesManager
from grader.tasks.redis_data_storage import RedisDataStorage


def _load_config() -> Dict[str, Any]:
    path = os.environ.get(ENV_VAR_CONFIG, None)
    if path and os.path.exists(path):
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
    else:
        config = dict()

    return config


def _create_tasks_manager() -> TasksManager:
    execution_backend = os.environ.get(ENV_VAR_EXECUTION_BACKEND, "docker")
    result_storage_url = os.environ.get(ENV_VAR_RESULT_STORAGE_URL, "redis://localhost:6379")

    result_storage = RedisDataStorage(prefix="", redis_client=redis.from_url(result_storage_url))
    if execution_backend == 'docker':
        manager = DockerBatchTasksManager(result_storage=result_storage)
    elif execution_backend == 'kubernetes':
        config_path = DEFAULT_WORKER_CONFIG_PATH
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        if 'kubernetes_manager' not in config:
            raise ValueError(f"No configuration for 'kubernetes_manager' in {config_path}")

        k8s_manager = KubernetesManager(**config)

        manager = KubernetesBatchTasksManager(result_storage=result_storage, namespace=k8s_manager.namespace)
    else:
        raise ValueError(f"Invalid value for {ENV_VAR_EXECUTION_BACKEND}: {execution_backend}. "
                         f"Only 'kubernetes' and 'docker' are supported.")

    return manager


_tasks_manager = _create_tasks_manager()


def tasks_manager() -> TasksManager:
    return _tasks_manager
