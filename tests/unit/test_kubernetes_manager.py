import uuid

import pytest
import redis
import yaml
from rnseism_sdk.envs import ENV_VAR_TOKEN, ENV_VAR_RUNNER_DB_CONN, ENV_VAR_LOGGING_LEVEL
from rnseism_sdk.sdk.data_storage import RedisDataStorage
from rnseism_sdk.worker.base import ParametersManager
from rnseism_sdk.worker.utils import TestSDKJob

from geowsm.env import ENV_VAR_TASK_ID, ENV_VAR_JOB_ID
from geowsm.tasks.base import NodeType, LABEL_RN_PROJECT_ID, LABEL_RN_ID
from geowsm.tasks.kubernetes.kubernetes_manager import LABEL_K8S_RN_OWNER, RNSEISM, \
    LABEL_K8S_RN_NODE_TYPE_LABEL_KEY, WORKER_CONFIG_CONFIG_MAP_KEY, KubernetesManager
from geowsm.tasks.nodes import NodeManagementException, VolumeSizeException, NoSuchVolumeException, \
    VolumeAlreadyExistsException


# These test require kubernetes cluster preparing to be running
# 1. Appropriate node labels should be set on chosen nodes (see sdkctl)
# 2. Appropriate folders for network storages should be prepared on NFS-based storage
# 3. NFS-based storage should be available on nodes (e.g. NetworkStorage)
# 4. Test namespace should be available
# 5. Test env databases should be accessible externally
# 6. worker config should reflect appropriate addresses for testing
# 7. Build and push docker images
# 8. ~/.kube/config should allow access to nodes info and for running pods

@pytest.fixture(scope='function')
def k8s_manager(k8s_ctx, k8s_network_storages) -> KubernetesManager:
    return KubernetesManager(
        network_storages=list(k8s_network_storages.values()),
        namespace=k8s_ctx.namespace
    )


@pytest.mark.skip("K8s cluster should be presented")
def test_list_nodes(k8s_ctx, k8s_manager):
    # TODO: better to test nodes requisites
    nodes = k8s_manager.list_nodes()
    assert len(nodes) == len(k8s_ctx.nodes.all)

    nodes = k8s_manager.list_nodes(node_type=NodeType.interactive)
    assert len(nodes) == len(k8s_ctx.nodes.interactive)

    nodes = k8s_manager.list_nodes(node_type=NodeType.compute)
    assert len(nodes) == len(k8s_ctx.nodes.compute)


@pytest.mark.skip("K8s cluster should be presented")
def test_set_unset_priority_project(k8s_ctx, k8s_manager):
    # no label
    nodes = k8s_manager.list_nodes()
    assert len(nodes) == len(k8s_ctx.nodes.all)
    assert all([node.priority_project_id is None for node in nodes])

    node_uid = k8s_ctx.nodes.all[0]
    project_uid = str(uuid.uuid4())
    k8s_manager.set_node_priority_project(node_uid=node_uid, project_uid=project_uid)
    node = k8s_manager.get_node(node_uid=node_uid)
    assert node is not None
    assert node.priority_project_id == project_uid

    # trying replace priority project uid
    other_project_uid = str(uuid.uuid4())

    with pytest.raises(NodeManagementException) as excinfo:
        k8s_manager.set_node_priority_project(node_uid=node_uid, project_uid=other_project_uid)
    assert 'the node already has priority' in excinfo.value.args[0]

    k8s_manager.set_node_priority_project(node_uid=node_uid, project_uid=other_project_uid, force=True)
    node = k8s_manager.get_node(node_uid=node_uid)
    assert node is not None
    assert node.priority_project_id == other_project_uid

    # trying unset priority project uid
    k8s_manager.unset_node_priority_project(node_uid=node_uid)
    node = k8s_manager.get_node(node_uid=node_uid)
    assert node is not None
    assert node.priority_project_id is None


@pytest.mark.skip("K8s cluster should be presented")
def test_storage_volumes(k8s_manager, k8s_network_storages, k8s_ctx):
    # checks volumes are empty
    volumes = k8s_manager.list_storage_volumes()
    assert len(volumes) == 0
    storage_volumes = k8s_manager.list_storages()
    assert len(storage_volumes) == 0
    storage_volumes = k8s_manager.list_storages(project_uid=str(uuid.uuid4()))
    assert len(storage_volumes) == len(k8s_network_storages)
    for storage_volume in storage_volumes:
        assert storage_volume.volume_uid is None
        assert storage_volume.volume_size_bytes == 0
        assert storage_volume.storage_size_bytes == k8s_network_storages[storage_volume.storage_uid].size_bytes

    ####################### first storage
    one_gi = 1024 * 1024 * 1024
    project_uid_1 = str(uuid.uuid4())
    project_uid_2 = str(uuid.uuid4())
    project_uid_3 = str(uuid.uuid4())
    nst_10g = next(nstorage for nstorage in k8s_network_storages.values() if nstorage.size_bytes == 10 * one_gi)
    nst_20g = next(nstorage for nstorage in k8s_network_storages.values() if nstorage.size_bytes == 20 * one_gi)

    # creating for first storage  - 1g, should be ok
    volume = k8s_manager.create_storage_volume(
        storage_uid=nst_10g.uid, project_uid=project_uid_1, size_bytes=one_gi
    )
    assert volume.size_bytes == one_gi and volume.storage_uid == nst_10g.uid and volume.project_uid == project_uid_1

    # creating for first storage  - 5g, cannot create second volume in the same storage for the same project
    with pytest.raises(VolumeAlreadyExistsException):
        volume = k8s_manager.create_storage_volume(
            storage_uid=nst_10g.uid, project_uid=project_uid_1, size_bytes=5 * one_gi
        )

    volume = k8s_manager.create_storage_volume(
        storage_uid=nst_10g.uid, project_uid=project_uid_2, size_bytes=5 * one_gi
    )
    assert volume.size_bytes == 5 * one_gi and volume.storage_uid == nst_10g.uid and volume.project_uid == project_uid_2

    # creating for first storage  - 5g, should thow error - not enough space
    with pytest.raises(VolumeSizeException) as excinfo:
        k8s_manager.create_storage_volume(
            storage_uid=nst_10g.uid, project_uid=project_uid_3, size_bytes=5 * one_gi
        )
    assert 'not enough' in excinfo.value.args[0].lower()

    # creating for first storage  - 4g, should be ok
    volume = k8s_manager.create_storage_volume(
        storage_uid=nst_10g.uid, project_uid=project_uid_3, size_bytes=4 * one_gi
    )
    assert volume.size_bytes == 4 * one_gi and volume.storage_uid == nst_10g.uid and volume.project_uid == project_uid_3

    #################### second storage

    # creating for second storage  - 16g, should be ok
    volume = k8s_manager.create_storage_volume(
        storage_uid=nst_20g.uid, project_uid=project_uid_1, size_bytes=16 * one_gi
    )
    assert volume.size_bytes == 16 * one_gi and volume.storage_uid == nst_20g.uid and volume.project_uid == project_uid_1

    # creating for first storage  - 15g, should throw error
    with pytest.raises(VolumeSizeException):
        k8s_manager.update_storage_volume(volume_uid=volume.uid, size_bytes=15 * one_gi)

    # creating for first storage  - 21g, should throw error
    with pytest.raises(VolumeSizeException):
        k8s_manager.update_storage_volume(volume_uid=volume.uid, size_bytes=21 * one_gi)

    # creating for first storage  - 18g, should be ok
    volume = k8s_manager.update_storage_volume(volume_uid=volume.uid, size_bytes=18 * one_gi)
    assert volume.size_bytes == 18 * one_gi and volume.storage_uid == nst_20g.uid and volume.project_uid == project_uid_1

    ############## check listing
    volumes = k8s_manager.list_storage_volumes()
    assert len(volumes) == 4
    assert {v.project_uid for v in volumes} == {project_uid_1, project_uid_2, project_uid_3}
    assert {(v.project_uid, v.storage_uid): v.size_bytes for v in volumes} == {
        (project_uid_1, nst_10g.uid): one_gi,
        (project_uid_2, nst_10g.uid): 5 * one_gi,
        (project_uid_3, nst_10g.uid): 4 * one_gi,
        (project_uid_1, nst_20g.uid): 18 * one_gi
    }

    volumes = k8s_manager.list_storage_volumes(project_uid=project_uid_1)
    assert len(volumes) == 2
    assert {v.project_uid for v in volumes} == {project_uid_1}

    nstorages = k8s_manager.list_storages()
    assert len(nstorages) == 4
    assert {(nst.project_uid, nst.storage_uid): nst.volume_size_bytes for nst in nstorages} == {
        (project_uid_1, nst_10g.uid): one_gi,
        (project_uid_2, nst_10g.uid): 5 * one_gi,
        (project_uid_3, nst_10g.uid): 4 * one_gi,
        (project_uid_1, nst_20g.uid): 18 * one_gi
    }
    assert {nst.storage_uid: nst.allocated_size_bytes for nst in nstorages} == {
        nst_10g.uid: 10 * one_gi,
        nst_20g.uid: 18 * one_gi
    }

    ############# check deleting
    with pytest.raises(NoSuchVolumeException):
        k8s_manager.delete_storage_volume(volume_uid=str(uuid.uuid4()))

    k8s_manager.delete_storage_volume(volume_uid=str(uuid.uuid4()), allow_missing=True)

    k8s_manager.delete_storage_volume(volume_uid=volumes[0].uid)

    volumes = k8s_manager.list_storage_volumes()
    assert len(volumes) == 3


@pytest.mark.skip("Needs refactoring")
@pytest.mark.parametrize("use_external_configmap", [False, True])
def test_successful_run_of_pod_with_integrations(auth_token,
                                                 db_ctx,
                                                 k8s_ctx,
                                                 k8s_manager,
                                                 k8s_network_storages,
                                                 pushable_batch_task_image,
                                                 use_external_configmap):
    worker_config, worker_config_path, _ = k8s_ctx.external_runner_config
    parameters_storage = ParametersManager(RedisDataStorage(redis.from_url(db_ctx.parameters_storage_url)))

    one_gi = 1024 * 1024 * 1024
    project_uid = str(uuid.uuid4())
    task_id = str(uuid.uuid4())
    pod_name = f"test-pod-{str(uuid.uuid4())}"

    param_result_keys = {
        'a': [124, 345, 67, 42.0],
        'b': 42.0,
        'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
        'd': False
    }

    parameters_storage.put(task_id, parameters={
        'duration': 10,
        'param_do_report_progress': False,
        'param_do_report_metrics': False,
        'param_result_keys': param_result_keys
    })

    for nst in k8s_network_storages.values():
        k8s_manager.create_storage_volume(
            storage_uid=nst.uid, project_uid=project_uid, size_bytes=4 * one_gi
        )

    if use_external_configmap:
        cm_name = f'external-configmap-{uuid.uuid4()}'
        config_map_body = {
            'metadata': {
                'name': cm_name,
                'labels': k8s_manager.base_labels()
            },
            'data': {
                WORKER_CONFIG_CONFIG_MAP_KEY: yaml.safe_dump(worker_config)
            }
        }
        k8s_manager.client.create_namespaced_config_map(
            namespace=k8s_manager.namespace,
            body=config_map_body
        )
        worker_config_args ={'worker_config_path': None, 'worker_config_configmap': cm_name}
    else:
        worker_config_args = {'worker_config_path': worker_config_path, 'worker_config_configmap': None}

    # launch with mounted storages, perform reading and writing there, accessing storage from there - should be ok
    k8s_manager.launch_configured_pod(
        project_id=project_uid,
        pod_name=pod_name,
        image=pushable_batch_task_image,
        labels={
            **k8s_manager.base_labels(),
            LABEL_RN_ID: task_id,
            LABEL_RN_PROJECT_ID: project_uid
        },
        node_selector={
            LABEL_K8S_RN_OWNER: RNSEISM,
            LABEL_K8S_RN_NODE_TYPE_LABEL_KEY: NodeType.compute.value
        },
        env={
            ENV_VAR_TOKEN: auth_token,
            ENV_VAR_TASK_ID: task_id,
            ENV_VAR_JOB_ID: TestSDKJob.__name__,
            ENV_VAR_RUNNER_DB_CONN: db_ctx.external_postgresql_url
        },
        cpu='2',
        memory=str(4 * one_gi),
        **worker_config_args
    )

    # wait for pod - should be ok
    k8s_manager.wait_for_pod(pod_name=pod_name, timeout_to_get_running=30, timeout=60)

    prefix = f"{task_id}."
    results = {k[len(prefix):]:v for k, v in db_ctx.result_storage.list(prefix=prefix).items()}
    assert 'task_out' in results
    assert 'hostname' in results['task_out'] and results['task_out']['hostname'] is not None
    del results['task_out']
    assert results == param_result_keys


@pytest.mark.skip("Needs refactoring")
def test_run_spark_pod(auth_token,
                       db_ctx,
                       k8s_ctx,
                       k8s_manager,
                       k8s_spark_config_map_name,
                       pushable_batch_task_image,
                       pushable_pyspark_executor_image):
    worker_config, worker_config_path, _ = k8s_ctx.external_runner_config
    parameters_storage = ParametersManager(RedisDataStorage(redis.from_url(db_ctx.parameters_storage_url)))

    one_gi = 1024 * 1024 * 1024
    project_uid = str(uuid.uuid4())
    task_id = str(uuid.uuid4())
    pod_name = f"test-spark-pod-{str(uuid.uuid4())}"

    parameters_storage.put(task_id, parameters=dict())

    worker_config_args = {'worker_config_path': worker_config_path, 'worker_config_configmap': None}

    # launch with mounted storages, perform reading and writing there, accessing storage from there - should be ok
    k8s_manager.launch_configured_pod(
        project_id=project_uid,
        pod_name=pod_name,
        image=pushable_batch_task_image,
        labels={
            **k8s_manager.base_labels(),
            LABEL_RN_ID: task_id,
            LABEL_RN_PROJECT_ID: project_uid
        },
        node_selector={
            LABEL_K8S_RN_OWNER: RNSEISM,
            LABEL_K8S_RN_NODE_TYPE_LABEL_KEY: NodeType.compute.value
        },
        env={
            ENV_VAR_TOKEN: auth_token,
            ENV_VAR_TASK_ID: task_id,
            ENV_VAR_JOB_ID: "SparkTestCustomOpsJob",
            # ENV_VAR_JOB_ID: "SparkTestJob",
            ENV_VAR_RUNNER_DB_CONN: db_ctx.external_postgresql_url,
            ENV_VAR_LOGGING_LEVEL: "DEBUG",
            "RNSEISM_SPARK_CLUSTER": "yes",
            "RNSEISM_SPARK_CONF_SPARK_KUBERNETES_DRIVER_POD_NAME": f"{pod_name}",
            "RNSEISM_SPARK_CONF_SPARK_KUBERNETES_DRIVER_MASTER": f"{pod_name}",
            # spark.driver.port
            "RNSEISM_SPARK_CONF_SPARK_DRIVER_HOST": f"{pod_name}",
            "RNSEISM_SPARK_CONF_SPARK_DRIVER_PORT": "39951",
            # spark.blockManager.port
            # "RNSEISM_SPARK_CONF_SPARK_DRIVER_PORT": "39570",
            "RNSEISM_CODEGEN_STEP_IMPORTER_PATH": "['file:///usr/local/lib/python3.10/site-packages/rnseism_sdk/spark/custom/custom_processors.py']"
        },
        cpu='2',
        memory=str(4 * one_gi),
        volumes=[
            {
                'volume': {
                    'name': 'spark-config',
                    'configMap': {
                        'name': k8s_spark_config_map_name
                    }
                },
                'volumeMount': {
                    'name': 'spark-config',
                    'mountPath': '/etc/spark/conf',
                    'readOnly': True
                }
            }
        ],
        service_type='NodePort',
        service_ports=[4040, 39951, 39570],
        service_account_name="spark",
        **worker_config_args
    )

    # wait for pod - should be ok
    k8s_manager.wait_for_pod(pod_name=pod_name, timeout_to_get_running=30, timeout=60)
