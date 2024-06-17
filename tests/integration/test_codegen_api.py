import logging
import os
from uuid import uuid4

import pytest

from geowsm.api.schemas.codegen import IntrospectContainerRequest, GetOpsRequest, CompileGraphRequest
from rnseism_sdk.db.tasks import TaskType
from rnseism_sdk.runner.base import FailedJobException
from rnseism_sdk.worker.utils import TestSDKJob

from geowsm.api.schemas.iworkers import IWorkerInfo, WorkerStatus
from geowsm.api.schemas.tasks import TaskInfoResponse, TaskStatus, TaskResultResponse, TaskLogResponse
from tests.fixtures.app import docker_app_context
from tests.utils import make_request, AppContext, wait_for_workers, wait_for_tasks, \
    wait_for_desired_num_of_active_workers, FailedRequestError

# TODO: refactor duplicated code after merge
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# "remote", "docker" or "k8s"
APP_CONTEXT = [el.lower().strip() for el in os.environ.get("GEOWSM_PYTEST_APP_CONTEXT", "docker").split(",")]


TEST_PARAMS = [
    ("blauncher_batch_task:test-geowsm", [])
]
GRAPHS = [
    # ("beam", "beam_graph_dump.yaml"),
    # ("spark", "gen_test_1_simple_graph.yaml"),
    # ("spark", "get_gather_1_simple.yaml"),
    # ("spark", "get_gather_2_no_reorder.yaml"),
    # ("spark", "get_gather_5_merge.yaml"),
    # ("spark", "get_volume_1_simple.yaml"),
    ("spark", "get_gather_0_read_write_overwrite.yaml"),
    ("spark", "get_volume_0_read_write_overwrite.yaml"),
    # ("spark", "get_volume_2_no_reorder.yaml"),
    # ("spark", "get_volume_3_inner_branch.yaml"),
    # ("spark", "get_volume_4_inner_split_merge.yaml"),
    # ("spark", "get_volume_5_merge.yaml"),
    # ("spark", "get_volume_7_multiple_outputs.yaml"),
]
# GRAPHS = [
#     ("beam", "beam_graph_dump.yaml"),
#     ("spark", "spark_graph_dump.yaml")
# ]


def make_payload(full_method_name, **params):
    return {
        "jsonrpc": "2.0",
        "id": 0,
        "method": full_method_name,
        "params": params,
    }


def make_codegen_payload(method_name, request):
    return make_payload(
        f"Codegen.{method_name}",
        request=request.dict()
    )


def validate_codegen_response(response):
    assert "is_success" in response, str(response)  # returned correct schema
    logger.debug(response) \
        if response["is_success"] \
        else logger.exception(response["error"])
    assert response["is_success"], response["error"]  # run successfully

    return response


@pytest.fixture(scope="module")
def yaml_graph(request) -> tuple[str, str]:
    graph_type, graph_name = request.param
    with open(f"data/{graph_name}", "r") as rf:
        yaml_str = rf.read()
    return graph_type, yaml_str


@pytest.fixture(scope="module")
def app_context(request, docker_app_context) -> AppContext:
    return docker_app_context
    if request.param == "docker":
        return request.getfixturevalue("docker_app_context")
    elif request.param == "k8s":
        return request.getfixturevalue("k8s_app_context")
    elif request.param == "remote":
        return request.getfixturevalue("remote_app_context")
    else:
        raise ValueError(f"Unsupported manager type: {request.param}")


@pytest.fixture(scope="module")
def image_introspection(request, app_context, yaml_graph):
    graph_type, _ = yaml_graph
    image_name, custom_ops_lib_paths = request.param
    introspection_payload = make_codegen_payload(
        "introspect",
        IntrospectContainerRequest(
            docker_image_name=image_name,
            custom_ops_lib_paths=custom_ops_lib_paths,
            pipeline_type=graph_type
        ))

    response = make_request(app_context, introspection_payload)
    validate_codegen_response(response)
    return {
        "introspection_response": response,
        "image_name": image_name,
        "custom_ops_lib_paths": custom_ops_lib_paths
    }


@pytest.mark.parametrize("app_context", APP_CONTEXT, indirect=True)
def test_dummy(app_context):
    print(app_context)
    assert True


@pytest.mark.parametrize("app_context", APP_CONTEXT, indirect=True)
@pytest.mark.parametrize("image_introspection", TEST_PARAMS, indirect=True)
@pytest.mark.parametrize("yaml_graph", GRAPHS, indirect=True)
def test_codegen_introspection(app_context, image_introspection, yaml_graph):
    graph_type, graph = yaml_graph
    image_name = image_introspection["image_name"]

    get_ops_payload = make_codegen_payload(
        "getOps",
        GetOpsRequest(docker_image_name=image_name)
    )
    response = make_request(app_context, get_ops_payload)
    validate_codegen_response(response)


@pytest.mark.parametrize("app_context", APP_CONTEXT, indirect=True)
@pytest.mark.parametrize("image_introspection", TEST_PARAMS, indirect=True)
@pytest.mark.parametrize("yaml_graph", GRAPHS, indirect=True)
def test_codegen_compilation(app_context, image_introspection, yaml_graph):
    image_name = image_introspection["image_name"]
    _, graph = yaml_graph

    compile_payload = make_codegen_payload(
        "compile",
        CompileGraphRequest(
            docker_image_name=image_name,
            graph_yaml=graph
        ))

    response = make_request(app_context, compile_payload)
    validate_codegen_response(response)


@pytest.mark.parametrize("app_context", APP_CONTEXT, indirect=True)
@pytest.mark.parametrize("image_introspection", TEST_PARAMS, indirect=True)
@pytest.mark.parametrize("yaml_graph", GRAPHS, indirect=True)
def test_pipeline_task_run(app_context, image_introspection, yaml_graph):
    image_name = image_introspection["image_name"]
    _, graph = yaml_graph

    run_payload = make_payload(
        "Task.start",
        task={
            "name": "codegen-run-pipeline-task",
            "task_type": TaskType.batch.value,
            "job_id": f"graph://{image_name}",
            "job_source": graph,
            "parameters": {},
            "session_uid": str(uuid4()),
            "priority": .0,
            "image": app_context.batch_task_image
        },
    )

    response = make_request(app_context, run_payload)
    wait_for_tasks(
        ctx=app_context,
        uids=[response['uuid']],
        desired_statuses=[TaskStatus.finished],
        timeout=120.0,
        retry_interval=5
    )


@pytest.mark.parametrize("app_context", APP_CONTEXT, indirect=True)
def test_spark_mock_job_task_run(app_context):

    run_payload = make_payload(
        "Task.start",
        task={
            "name": "test-spark-task",
            "task_type": TaskType.batch.value,
            "job_id": f"MockSparkJob",
            "parameters": {},
            "session_uid": str(uuid4()),
            "priority": .0,
            "image": app_context.batch_task_image
        },
    )

    response = make_request(app_context, run_payload)

