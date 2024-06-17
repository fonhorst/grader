import datetime
import logging
import os
import pprint
import socket
import subprocess
import time
import uuid
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, Any, Union, List, Tuple, Optional, cast, TypeVar, Type
from urllib.parse import urlparse, urljoin

import requests
from docker.models.containers import Container
from pydantic import BaseModel
from requests.adapters import HTTPAdapter
from rnseism_sdk.db.tasks import TaskStatus
from rnseism_sdk.worker.utils import TestSDKJob
from urllib3 import Retry

from geowsm.api.schemas.iworkers import WorkerStatus, IWorkerInfo
from geowsm.api.schemas.tasks import TaskInfoResponse
from geowsm.env import JSONRPC_API_ROOT
from geowsm.tasks.base import TasksManager, TaskAndResult, TaskResult, RNSEISM_BATCH_WORKER
from geowsm.tasks.interactive_tasks import InteractiveTasksManager, InteractiveTaskRunArgs
from geowsm.tasks.manager import CompositeTasksManager

logger = logging.getLogger(__name__)


# Test container configuration set for JWT auth ignore, so executing as a single user now.
# Change this if you will need a multiple user scenario.
# APP_AUTH_HEADER just needs to be in request, but value not using until JWT auth is disabled.
APP_AUTH_HEADER = {}  # {"Authorization": "Bearer ..."}
IMAGE_TAG = "test-geowsm"

T = TypeVar("T", bound=BaseModel)

@dataclass
class Ctx:
    token: str
    project_id: uuid.UUID
    user_id: uuid.UUID
    jobs_path: str

    worker_hostname: str
    worker_config: Dict[str, Any]
    param_kwargs: Dict[str, Any]

    run_args: InteractiveTaskRunArgs


def replace_hostname(url: str, host: str) -> str:
    a = urlparse(url)
    netloc = f"{a.username or ''}{':' + a.password if a.password else ''}{'@' if a.username else ''}" \
             f"{host}{':' + str(a.port) if a.port else ''}"
    return a._replace(netloc=netloc).geturl()


def get_ip(node: str):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect((node, 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip

# Invoking this fixture: 'function_scoped_container_getter' starts all
# servicesw
def get_session():
    request_session = requests.Session()
    retries = Retry(backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    request_session.mount("http://", HTTPAdapter(max_retries=retries))
    return request_session


@dataclass
class AppContext:
    session: requests.Session
    api_url: str
    token: str
    user_id: str
    project_id: str
    worker_image: str
    batch_task_image: Optional[str]


class FailedRequestError(Exception):
    pass


def make_request(api: Union[str, AppContext], payload: Dict[str, Any], token: Optional[str] = None) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    assert "method" in payload, f"Invalid payload. Probably it is not a body of request for WMS: {payload}"

    session = api.session if isinstance(api, AppContext) else get_session()
    api_url = api.api_url if isinstance(api, AppContext) else api
    if token is None:
        token = api.token if isinstance(api, AppContext) else ''

    response = session.post(
        urljoin(api_url, JSONRPC_API_ROOT),
        json=payload,
        headers={"Jsonrpc-Method": payload["method"], "authorization": token},
        timeout=15,
    )

    assert response.status_code == 200
    body = response.json()
    if 'error' in body:
        raise FailedRequestError(f"{body['error']['code']} {body['error']['message']}")
    if 'result' not in body:
        logger.error(f"Received response: \n{pprint.pformat(body)}")
        raise Exception("Erroneous response. See the log for further details")
    else:
        logger.info(f"Received response: \n{pprint.pformat(body)}")

    return body['result']


def push_image(docker_repo_url: str, image: str) -> str:
    image_name = f"{docker_repo_url}/{image}"

    logger.info(f"Pushing image {image_name}")

    proc = subprocess.run(
        f"docker tag {image} {image_name} && docker push {image_name}",
        shell=True
    )

    if proc.returncode != 0:
        raise RuntimeError(f"Failed to push worker image {image_name}")

    logger.info(f"Successfully pushed image {image_name}")

    return image_name


def extract_user_and_project(pytestconfig, token: str) -> Tuple[str, str]:
    jwt_private_key_path = os.path.join(str(pytestconfig.rootpath), "deployment/rsa_test_keys/jwtRS256.key")
    jwt_private_key_container_path = "/jwt_key"
    # extract user_id and project_id
    os.environ["JWT_PRIVATE_KEY_PATH"] = jwt_private_key_path

    from rnseism.jwt import JWT
    decoded_token = JWT.decode_token(token)
    user_id = decoded_token['userUuid']
    project_id = decoded_token['projectUuid']
    return user_id, project_id


def check_logs(manager: InteractiveTasksManager, worker_uid: str, timeout: Optional[float] = None):
    begin = datetime.datetime.now()

    while True:
        logs = manager.get_worker_log(worker_uid)

        cond = (logs is not None and len(logs) > 0)

        if cond:
            break

        elapsed = int((datetime.datetime.now() - begin).total_seconds())

        if not timeout or elapsed  > timeout:
            break

        time.sleep(0.1)

    elapsed = (datetime.datetime.now() - begin).total_seconds()
    logger.info("Took: %s. Condition: %s" % (elapsed, cond))

    assert cond


def check_tasks_log(manager: TasksManager,
                    atasks: List[TaskAndResult],
                    desired_content_part: Optional[Union[str, List[str]]] = None):
    for atask in atasks:
        if isinstance(manager, CompositeTasksManager):
            log = manager.get_log(atask.task.uid)
        else:
            log = manager.get_log(atask.task.task_uid)
        assert log is not None
        if desired_content_part:
            desired_content_part = [desired_content_part] \
                if isinstance(desired_content_part, str) else desired_content_part
            for part in desired_content_part:
                assert part in log


def check_tasks_status(manager: TasksManager,
                       atasks: List[TaskAndResult],
                       desired_statuses: Union[TaskStatus, List[TaskStatus]],
                       timeout: Optional[float] = None):

    desired_statuses = [desired_statuses] if isinstance(desired_statuses, TaskStatus) else desired_statuses

    begin = datetime.datetime.now()

    while True:
        tasks = manager.list()
        requires_tasks_uids = [atask.task.uid for atask in atasks]
        required_tasks = [
            task for task in tasks
            if task.uid in requires_tasks_uids
        ]
        assert len(required_tasks) == len(requires_tasks_uids)

        elapsed = int((datetime.datetime.now() - begin).total_seconds())

        if elapsed % 2 == 0:
            counter = Counter([task.status for task in required_tasks])
            logger.info("Current statuses: %s" % counter)

        cond = all(task.status in desired_statuses for task in required_tasks)

        if cond:
            break

        if not timeout or elapsed  > timeout:
            break

        time.sleep(0.1)

    elapsed = (datetime.datetime.now() - begin).total_seconds()
    logger.info("Took: %s. Statuses: %s" % (elapsed, Counter([task.status for task in required_tasks])))

    assert cond


def check_task_result(atask: TaskAndResult, worker_hostname: str, timeout: int = 10):
    result = atask.result.get(timeout=timeout)
    result = TaskResult.parse_obj(result)
    assert result.result == {"field_1": "some answer #1", "field_2": 42, "hostname": worker_hostname}


def remove_containers(client, labels: List[str], all_containers: bool = True):
    containers = (
        cast(Container, cnt)
        for label in labels for cnt in client.containers.list(all=all_containers, filters={'label': [label]})
    )

    for cnt in containers:
        if cnt.status in ['running', 'restarting']:
            cnt.kill()
        cnt.remove()


def make_run_args(run_args: InteractiveTaskRunArgs,
                  duration: float,
                  session_id: Optional[str] = None,
                  do_fail: bool = False) -> InteractiveTaskRunArgs:
    ra = deepcopy(run_args)
    # ra.task_id = str(uuid.uuid4()) if generate_new_uuid else ra.task_id
    ra.parameters['duration'] = duration
    ra.parameters['param_do_fail'] = do_fail
    if session_id:
        ra.session_id = session_id
    return ra


def wait_for_entities(ctx: AppContext,
                      method: str,
                      entity_clazz: Type[T],
                      uids: List[str],
                      desired_statuses: List[Any],
                      timeout: float = 10.0,
                      retry_interval: float = 0.1) -> List[T]:
    begin = datetime.datetime.now()

    entities_with_desired_status = dict()

    while True:
        # TODO: replace with list method
        for uid in uids:
            if uid in entities_with_desired_status:
                continue

            payload = {
                "jsonrpc": "2.0",
                "id": 0,
                "method": method,
                "params": {
                    "uid": uid,
                    **({'tunings': {'includeReason': True}} if method == 'Task.get' else dict())
                },
            }
            response = make_request(ctx, payload=payload)
            entity = entity_clazz.parse_obj(response)

            if entity.status in desired_statuses:
                entities_with_desired_status[entity.uid] = entity
            elif entity.status.is_terminal():
                raise ValueError(f"Found entity in terminal undesired state ({entity.status}). No point to continue. "
                                 f"Reason: {entity.reason if hasattr(entity, 'reason') else None}")

        if len(entities_with_desired_status) == len(uids):
            return list(entities_with_desired_status.values())

        if (datetime.datetime.now() - begin).total_seconds() > timeout:

            raise ValueError("Timeout is exceeded")

        time.sleep(retry_interval)


def wait_for_workers(
        ctx: AppContext,
        uids: List[str],
        desired_statuses: List[WorkerStatus],
        timeout: float = 10.0,
        retry_interval: float = 0.1) -> List[IWorkerInfo]:
    result = wait_for_entities(
        ctx,
        method="IWorkers.get",
        entity_clazz=IWorkerInfo,
        uids=uids,
        desired_statuses=desired_statuses,
        timeout=timeout,
        retry_interval=retry_interval
    )
    return result


def wait_for_tasks(
        ctx: AppContext,
        uids: List[str],
        desired_statuses: List[TaskStatus],
        timeout: float = 10.0,
        retry_interval: float = 0.1) -> List[TaskInfoResponse]:
    result = wait_for_entities(
        ctx,
        method="Task.get",
        entity_clazz=TaskInfoResponse,
        uids=uids,
        desired_statuses=desired_statuses,
        timeout=timeout,
        retry_interval=retry_interval
    )
    return result


def wait_for_desired_num_of_active_workers(
        ctx: AppContext,
        desired_num_of_active_workers: int,
        timeout: float = 10.0,
        retry_interval: float = 0.1):
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "IWorkers.list",
        "params": {
            "filter": {},
            "tunings": {},
            "navigation": {}
        },
    }

    begin = datetime.datetime.now()

    while True:
        response = make_request(ctx, payload=payload)
        workers = [IWorkerInfo.parse_obj(r) for r in response]
        active_workers = [w for w in workers if w.status == WorkerStatus.running]

        if len(active_workers) == desired_num_of_active_workers:
            return

        if (datetime.datetime.now() - begin).total_seconds() > timeout:
            raise ValueError("Timeout is exceeded")

        time.sleep(retry_interval)


def wait_for_desired_workers_count(manager, project_id, count, timeout: int = 10):
    # TODO: kubernetes may take a few time to delete the pod
    begin = datetime.datetime.now()
    while True:
        elapsed_time = (datetime.datetime.now() - begin).total_seconds()
        cnts = manager.list_workers(project_id)
        cond = len(cnts) == count

        if cond or elapsed_time > timeout:
            break

        time.sleep(0.1)

    assert cond


def make_ctx(pytestconfig, runner_config, auth_token: str):
    user_id, project_id = extract_user_and_project(pytestconfig, auth_token)
    try:
        worker_config, _, _ = runner_config
    except:
        worker_config, _ = runner_config
    worker_hostname = "worker-1"

    param_kwargs = {
        'duration': 5,
        'param_result_keys': {
            'a': [124, 345, 67, 42.0],
            'b': 42.0,
            'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
            'd': False
        },
        'task_out': {"field_1": "some answer #1", "field_2": 42, "hostname": worker_hostname}
    }

    run_args = InteractiveTaskRunArgs(
        name="some_test_interactive_task",
        user_id=str(user_id),
        project_id=str(project_id),
        job_id=TestSDKJob.__name__,
        parameters=param_kwargs
    )

    return Ctx(
        token=auth_token,
        project_id=uuid.UUID(project_id),
        user_id=uuid.UUID(user_id),
        worker_hostname=worker_hostname,
        worker_config=worker_config,
        jobs_path=worker_config['jobs_path'],
        param_kwargs=param_kwargs,
        run_args=run_args,
    )


def log_to_file(content: Union[str, bytes], filename: str, dir: str = ''):
    PREFIX = 'logs'

    file_path = os.path.join(PREFIX, dir, filename)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    if isinstance(content, bytes):
        with open(file_path, 'wb') as f:
            f.write(content)
    elif isinstance(content, str):
        with open(file_path, 'w') as f:
            f.write(content)
    else:
        raise ValueError("Content must be a string or bytes")


def only_for_k8s(contexts: List[str]) -> List[str]:
    return [ctx for ctx in contexts if ctx == 'k8s']
