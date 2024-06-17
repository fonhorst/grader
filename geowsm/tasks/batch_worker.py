import logging
import os
from copy import copy

import redis
import yaml
from celery.signals import worker_init

from geowsm.tasks.app import make_app
from rnseism_sdk.worker.base import ParametersManager
from geowsm.tasks.kubernetes.kubernetes_manager import KubernetesManager
from .nodes import NetworkStorage
from .tasks import run_docker_batch_task, run_kubernetes_batch_task, set_current_parameters_manager, \
    get_batch_worker_type, set_current_kubernetes_manager, BatchWorkerType
from rnseism_sdk.envs import DEFAULT_WORKER_CONFIG_PATH, ENV_VAR_WORKER_CONFIG_PATH
from rnseism_sdk.sdk.data_storage import RedisDataStorage

logger = logging.getLogger(__name__)

celery_app = make_app()
run_docker_batch_task = run_docker_batch_task
run_kubernetes_batch_task = run_kubernetes_batch_task


@worker_init.connect
def at_start(sender, **_):
    logger.warning(f'{sender} worker has started')

    config_path = os.environ.get(ENV_VAR_WORKER_CONFIG_PATH, DEFAULT_WORKER_CONFIG_PATH)
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        wtype = get_batch_worker_type()

        if not wtype:
            raise ValueError("Batch worker type is not defined. May be {ENV_VAR_BATCH_WORKER_TYPE} is not set.")

        client = redis.from_url(config['parameters_manager'])
        storage = RedisDataStorage(client)

        pstorage = ParametersManager(storage)
        set_current_parameters_manager(pstorage)

        if wtype == BatchWorkerType.kubernetes:
            cfg = copy(config['kubernetes_manager'])
            network_storages = [NetworkStorage.parse_obj(s) for s in cfg['network_storages']]
            del cfg['network_storages']
            k8s_manager = KubernetesManager(network_storages=network_storages, **cfg)
            set_current_kubernetes_manager(k8s_manager)

        logger.warning("Worker is initialized")
    except Exception:
        logger.error("Exception in worker start event", exc_info=True)
        raise
