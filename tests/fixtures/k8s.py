import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass, astuple
from pathlib import Path
from typing import Optional, List, Dict, Any

import pytest
import yaml
from kubernetes import client, config
from kubernetes.client import ApiException
from urllib3.exceptions import MaxRetryError

from geowsm.env import CM_NAME
from geowsm.tasks.base import NodeType
from geowsm.tasks.kubernetes.kubernetes_manager import LABEL_K8S_RN_OWNER, RNSEISM, LABEL_K8S_RN_NODE_TYPE_LABEL_KEY, \
    LABEL_K8S_RN_PRIORITY_PROJECT_ID, WORKER_CONFIG_CONFIG_MAP_KEY, _TEN_GYGABYTES
from geowsm.tasks.nodes import NetworkStorage
from tests.fixtures.db import DBCtx

logger = logging.getLogger(__name__)


@dataclass
class K8SNodes:
    interactive: List[str]
    compute: List[str]

    @property
    def all(self) -> List[str]:
        return [*self.interactive, *self.compute]


@dataclass
class RunnerConfig:
    config: Dict[str, Any]
    config_filename: str
    configmap_name: str

    def __iter__(self):
        return iter(astuple(self))


@dataclass
class K8SContext:
    config: str
    client: client.CoreV1Api
    namespace: str
    nodes: K8SNodes
    manager_config: Dict[str, Any]
    external_runner_config: RunnerConfig


@pytest.fixture(scope='session')
def k8s_config() -> Optional[str]:
    # return None
    return os.path.join(Path.home(), ".kube", "config")


@pytest.fixture(scope='session')
def k8s(k8s_config) -> Optional[client.CoreV1Api]:
    logger.info("Preparing k8s client")

    config.load_kube_config(config_file=k8s_config)
    k8s_client = client.CoreV1Api()

    # if not available test will fail
    try:
        k8s_client.list_node(timeout_seconds=1)
    except MaxRetryError as ex:
        raise Exception("It seems kubernetes cluster is not available") from ex

    logger.info("k8s client has been prepared")

    return k8s_client


@pytest.fixture(scope='session')
def k8s_nodes(k8s) -> Optional[K8SNodes]:
    if not k8s:
        yield None
        return

    logger.info("Labeling k8s nodes")

    interactive_nodes = ["node19.bdcl"]
    compute_nodes = ["node21.bdcl", "node22.bdcl", "node25.bdcl"]
    all_nodes = [*interactive_nodes, *compute_nodes]

    # set labels for compute nodes
    for label_val, nodes in [(NodeType.compute.value, compute_nodes), (NodeType.interactive.value, interactive_nodes)]:
        body = {
            "metadata": {
                "labels": {
                    LABEL_K8S_RN_OWNER: RNSEISM,
                    LABEL_K8S_RN_NODE_TYPE_LABEL_KEY: label_val
                }
            }
        }
        for node in nodes:
            k8s.patch_node(name=node, body=body)

    logger.info("k8s has been labeled")

    yield K8SNodes(interactive=interactive_nodes, compute=compute_nodes)

    body = {
        "metadata": {
            "labels": {
                LABEL_K8S_RN_OWNER: None,
                LABEL_K8S_RN_NODE_TYPE_LABEL_KEY: None,
                LABEL_K8S_RN_PRIORITY_PROJECT_ID: None
            }
        }
    }
    for node in nodes:
        k8s.patch_node(name=node, body=body)


@pytest.fixture(scope='session')
def k8s_namespace(k8s) -> Optional[str]:
    if not k8s:
        yield None
        return

    ns_name = os.environ.get("GEOWSM_PYTEST_K8S_NAMESPACE", 'rnseism-test')

    logger.info("Preparing k8s namespace: %s" % ns_name)

    def _clean_ns():
        # k8s.delete_namespace(name=ns_name, grace_period_seconds=0, propagation_policy='Foreground')
        subprocess.run(
            f"kubectl -n {ns_name} delete -l {LABEL_K8S_RN_OWNER}={RNSEISM} "
            f"pods,replicasets,statefulsets,deployments,services,configmaps",
            shell=True, check=True
        )

        volumes = k8s.list_persistent_volume(label_selector=f'{LABEL_K8S_RN_OWNER}={RNSEISM}').items
        for volume in volumes:
            k8s.delete_persistent_volume(name=volume.metadata.name)

    _clean_ns()

    # shouild be created earlier
    # body= {
    #     'metadata': {
    #         'name': ns_name,
    #         'labels': {
    #             LABEL_K8S_RN_OWNER: RNSEISM
    #         }
    #     }
    # }
    # k8s.create_namespace(body=body)

    logger.info("k8s namespace has been prepared")

    yield ns_name

    _clean_ns()

@pytest.fixture(scope='session')
def k8s_network_storages_config() -> Optional[Dict[str, NetworkStorage]]:
    return {
        'astorage': NetworkStorage(
            uid="astorage",
            size_bytes=10 * 1024 * 1024 * 1024,
            network_storage_base_host_path="/mnt/ess_storage/DN_1/tmp/test_rnseism_storages/storage_a"
        ),
        'bstorage': NetworkStorage(
            uid="bstorage",
            size_bytes=20 * 1024 * 1024 * 1024,
            network_storage_base_host_path="/mnt/ess_storage/DN_1/tmp/test_rnseism_storages/storage_b"
        )
    }


@pytest.fixture(scope='function')
def k8s_network_storages(k8s, k8s_namespace, k8s_network_storages_config) \
        -> Optional[Dict[str, NetworkStorage]]:
    if not k8s:
        yield None
        return

    logger.info("Preparing k8s network storages")

    volumes = k8s.list_namespaced_persistent_volume_claim(
        namespace=k8s_namespace,
        label_selector=f'{LABEL_K8S_RN_OWNER}={RNSEISM}'
    ).items
    for volume in volumes:
        k8s.delete_namespaced_persistent_volume_claim(namespace=k8s_namespace, name=volume.metadata.name)

    volumes = k8s.list_persistent_volume(label_selector=f'{LABEL_K8S_RN_OWNER}={RNSEISM}').items
    for volume in volumes:
        k8s.delete_persistent_volume(name=volume.metadata.name)

    logger.info("k8s network storages has been prepared")

    yield k8s_network_storages_config


@pytest.fixture(scope='session')
def k8s_manager_config(k8s, k8s_namespace, k8s_network_storages_config) \
        -> Optional[Dict[str, Any]]:
    if not k8s:
        return None

    return {
        'network_storages': [s.dict() for s in k8s_network_storages_config.values()],
        'namespace': k8s_namespace,
        'scratch_size_bytes': _TEN_GYGABYTES,
        'k8s_filelock_timeout': 5,
        'k8s_operation_timeout': 5,
        'k8s_default_config_map': CM_NAME
    }


@pytest.fixture(scope='session')
def k8s_spark_config_map_name(k8s, k8s_namespace, pushable_pyspark_executor_image) -> Optional[str]:
    if not k8s:
        return None

    config_map_name = 'spark-conf'

    # todo: set these settings in the init_spark of rnseism_sdk
    # todo: spark.kubernetes.driver.pod.name
    # todo: 'spark.kubernetes.driver.master',
    config_map_body = {
        'metadata': {
            'name': config_map_name,
            'labels': {
                LABEL_K8S_RN_OWNER: RNSEISM
            }
        },
        'data': {
            # todo: correct executor image
            'spark-defaults.conf': f"""
spark.master=k8s://https://node2.bdcl:6443
spark.executor.memory=16g
spark.executor.cores=8
spark.executor.instances=2
spark.kubernetes.executor.container.image={pushable_pyspark_executor_image}
spark.kubernetes.container.image.pullPolicy=Always
spark.kubernetes.executor.deleteOnTermination=false
spark.kubernetes.namespace=rnseism-test
spark.blockManager.port=39570
"""
        }
    }

    k8s.create_namespaced_config_map(
        namespace=k8s_namespace,
        body=config_map_body
    )

    return config_map_name


@pytest.fixture(scope='session')
def k8s_external_runner_config(
        k8s,
        k8s_namespace,
        k8s_manager_config,
        db_ctx: DBCtx) -> Optional[RunnerConfig]:
    if not k8s:
        yield None
        return

    logger.info("Preparing k8s runner config")

    # TODO: make unification with runner_config later
    result_storage_url = f"{db_ctx.external_redis_url}/2"
    state_storage_url = f"{db_ctx.external_redis_url}/3"
    session_storage_url = f"{db_ctx.external_redis_url}/4"
    parameters_storage_url = f"{db_ctx.external_redis_url}/5"

    worker_config = {
        'jobs_path': '/jobs',
        'jobs': {},
        'parameters_manager': parameters_storage_url,
        'kubernetes_manager': k8s_manager_config,
        'runner': {
            'db_url': db_ctx.external_postgresql_url,
            'storage_url': db_ctx.storage_url,
            'result_storage_url': result_storage_url,
            'state_storage_url': state_storage_url,
            'session_status_storage_url': session_storage_url
        }
    }

    config_map_body = {
        'metadata': {
            'name': CM_NAME,
            'labels': {
                LABEL_K8S_RN_OWNER: RNSEISM
            }
        },
        'data': {
            WORKER_CONFIG_CONFIG_MAP_KEY: yaml.safe_dump(worker_config)
        }
    }
    k8s.create_namespaced_config_map(
        namespace=k8s_namespace,
        body=config_map_body
    )

    with tempfile.NamedTemporaryFile(mode='w+') as fp:
        yaml.dump(worker_config, fp)
        yield RunnerConfig(config=worker_config, config_filename=fp.name, configmap_name=CM_NAME)

    logger.info("k8s runner config has been prepared")


@pytest.fixture(scope='session')
def k8s_ctx(
        k8s,
        k8s_config,
        k8s_manager_config,
        k8s_nodes,
        k8s_namespace,
        k8s_network_storages_config,
        k8s_external_runner_config,
        k8s_spark_config_map_name
) -> Optional[K8SContext]:
    if k8s:
        return K8SContext(
            config=k8s_config,
            client=k8s,
            namespace=k8s_namespace,
            nodes=k8s_nodes,
            manager_config=k8s_manager_config,
            external_runner_config=k8s_external_runner_config
        )
    return None
