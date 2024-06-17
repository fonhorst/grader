import ast
import logging
import pprint
import socket
from dataclasses import dataclass
from datetime import datetime
from itertools import zip_longest
from typing import Optional, Dict, Any, List, Literal
from urllib.parse import urljoin, urlparse
from uuid import UUID

import requests
import requests_toolbelt.utils.dump
from fastapi import Request, Response, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose.exceptions import ExpiredSignatureError, JWTError
from pydantic import BaseModel, Extra
from requests.adapters import HTTPAdapter
from rnseism.jwt import JWT
from rnseism.jwt.exceptions import JsonrpcError

from geowsm.db.codegen_introspection import CodegenIntrospection
from geowsm.api.services.codegen_introspect_service import CodegenIntrospectionService
from geowsm.api.schemas.jobs import JobType
from geowsm.tasks.base import TaskInfo
from urllib3 import Retry

from geowsm.api.base import handle_tunings_mode, project_and_users_api_url
from geowsm.api.schemas.tasks import TaskTunings, NodeInfo, ResourceInfo, ResourceUnitsEnum, BatchTaskRequest
from geowsm.env import JSONRPC_API_ROOT
from rnseism_sdk.db.tasks import TaskType
from rnseism_sdk.sdk.env import ENV_VAR_TOKEN

logger = logging.getLogger(__name__)


@dataclass
class TokenInfo:
    token: str
    user_id: str
    project_id: Optional[str] = None


def get_token(
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False)),
        request: Request = None,
        response: Response = None) -> Optional[TokenInfo]:
    if not credentials:
        return None

    token = credentials.credentials.replace("Bearer ", "")
    try:
        decoded = JWT.decode_token(token)
    except ExpiredSignatureError:
        raise JsonrpcError(
            code=-32401,
            message="Token is expired"
        )
    except JWTError:
        raise JsonrpcError(
            code=-32401,
            message="Token is invalid"
        )

    if "userId" in decoded.keys():
        requester_id = int(decoded.get("userId"))
        requester_uuid = UUID(int=requester_id)
    elif "userUuid" in decoded.keys():
        requester_uuid = UUID(decoded.get("userUuid"))
        requester_id = int(requester_uuid)
    else:
        raise JsonrpcError(
            code=-32401,
            message="Token doesn't contain user identification."
        )

    if "projectUuid" in decoded.keys():
        project_uuid = UUID(decoded.get("projectUuid"))
        project_id = int(project_uuid)
    elif "projectId" in decoded.keys():
        project_id = int(decoded.get("projectId"))
        project_uuid = UUID(int=project_id)
    else:
        raise JsonrpcError(
            code=-32422,
            message="Token doesn't contain project uuid."
        )

    return TokenInfo(
        token=token,
        user_id=str(requester_uuid),
        project_id=str(project_uuid)
    )


class ProjectInfo(BaseModel):
    uuid: str
    name: str
    description: str

    class Config:
        extra = Extra.ignore

    def to_args(self) -> Dict[str, str]:
        return {
            'project_name': self.name,
            'project_info': self.description
        }


class UserInfo(BaseModel):
    uuid: str
    login: str
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    middleName: Optional[str] = None

    class Config:
        extra = Extra.ignore

    def to_args(self) -> Dict[str, str]:
        return {
            'author_name': f"{self.lastName} {self.firstName} {self.middleName}"
        }


def get_session():
    request_session = requests.Session()
    retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    request_session.mount("http://", HTTPAdapter(max_retries=retries))
    return request_session


def make_request(api_url: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    response = get_session().post(
        urljoin(api_url, JSONRPC_API_ROOT),
        json=payload,
        headers={"Jsonrpc-Method": payload["method"]},
        timeout=15,
    )

    logger.debug("Sent requests: %s" % requests_toolbelt.utils.dump.dump_all(response).decode('utf-8'))

    assert response.status_code == 200
    body = response.json()

    if 'error' in body and body['error'] == -32404:
        return None

    if 'result' not in body:
        logger.error(f"Received response: \n{pprint.pformat(body)}")
        raise ErrorToRetrieveInfoFromExternalService(f"Cannot execute query. Payload: {payload}. Response body: {body}.")
    else:
        logger.info(f"Received response: \n{pprint.pformat(body)}")

    return body['result']


def _make_project_info_mock(project_uid: str):
    return {
        'uuid': project_uid,
        'name': 'Unknown name',
        'description': 'Unknown description'
    }


def _make_user_info_mock(user_uid: str):
    return {
        'uuid': user_uid,
        'login': 'UnknownLogin',
        'firstName': 'UnknownFirstName',
        'lastName': 'UnknownLastName',
        'middleName': 'UnknownMiddleName'
    }


def get_project_info(project_uid: str) -> Optional[ProjectInfo]:
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Project.get",
        "params": {
            "tunings": {},
            "uuid": project_uid
        }
    }

    if handle_tunings_mode == 'mock':
        body = _make_project_info_mock(project_uid)
    else:
        body = make_request(project_and_users_api_url, payload)

    if not body and handle_tunings_mode == 'mock_if_not_found':
        body = _make_project_info_mock(project_uid)

    return ProjectInfo.parse_obj(body) if body else None


def check_project_exists(project_uid: str):
    info = get_project_info(project_uid)

    if not info:
        raise DependencyNotFound(f"Project with uuid {project_uid} not found")


def get_multiple_project_infos(project_uuids: List[str]) -> List[ProjectInfo]:
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Project.list",
        "params": {
            "filter": {
                "uuid": {"$in": project_uuids}
            },
            "tunings": {},
            "navigation": {}
        }
    }

    if handle_tunings_mode == 'mock':
        body = [_make_project_info_mock(uid) for uid in project_uuids]
    else:
        body = make_request(project_and_users_api_url, payload)

    pinfos = (ProjectInfo.parse_obj(elt) for elt in body)
    pinfos = {pinfo.uuid: pinfo for pinfo in pinfos}

    presult = []
    for uid in project_uuids:
        if uid in pinfos:
            pinfo = pinfos[uid]
        elif handle_tunings_mode == 'mock_if_not_found':
            pinfo = _make_project_info_mock(uid)
        else:
            raise DependencyNotFound(f"Project with uuid {uid} not found")
        presult.append(pinfo)

    return presult


def get_user_info(user_uid: str) -> Optional[UserInfo]:
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "User.get",
        "params": {
            "tunings": {},
            "uuid": user_uid
        }
    }

    if handle_tunings_mode == 'mock':
        body = _make_user_info_mock(user_uid)
    else:
        body = make_request(project_and_users_api_url, payload)

    if not body and handle_tunings_mode == 'mock_if_not_found':
        body = _make_project_info_mock(user_uid)

    return UserInfo.parse_obj(body) if body else None


def check_user_exists(user_uid: str):
    info = get_user_info(user_uid)

    if not info:
        raise DependencyNotFound(f"User with uuid {user_uid} not found")


def get_multiple_user_infos(user_uuids: List[str]) -> List[UserInfo]:
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "User.list",
        "params": {
            "filter": {
                "uuid": {"$in": user_uuids}
            },
            "tunings": {},
            "navigation": {}
        }
    }

    if handle_tunings_mode == 'mock':
        body = [_make_user_info_mock(uid) for uid in user_uuids]
    else:
        body = make_request(project_and_users_api_url, payload)

    pinfos = (UserInfo.parse_obj(elt) for elt in body)
    pinfos = {pinfo.uuid: pinfo for pinfo in pinfos}

    presult = []
    for uid in user_uuids:
        if uid in pinfos:
            pinfo = pinfos[uid]
        elif handle_tunings_mode == 'mock_if_not_found':
            pinfo = _make_user_info_mock(uid)
        else:
            raise DependencyNotFound(f"User with uuid {uid} not found")
        presult.append(pinfo)

    return presult


def completion_time_to_remaining_sec(deadline: Optional[datetime]) -> Optional[float]:
    return deadline and (deadline - datetime.now()).total_seconds()


def get_ip(hostname: str) -> str:
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror as ex:
        logger.warning("Failed to determine ip for hostname %s due to %s" % (hostname, ex))
        return 'Unknown ip'


def handle_tunings(task: TaskInfo, tunings: Optional[TaskTunings]) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = dict()

    if not tunings:
        return kwargs

    if tunings.include_author:
        user_info = get_user_info(task.user_id)
        if not user_info:
            raise DependencyNotFound(f"User with uuid {task.project_id} not found")
        kwargs.update(user_info.to_args())

    if tunings.include_project:
        project_info = get_project_info(task.project_id)
        if not project_info:
            raise DependencyNotFound(f"Project with uuid {task.project_id} not found")
        kwargs.update(project_info.to_args())

    if tunings.include_nodes:
        kwargs['nodes'] = [
            NodeInfo(
                name=socket.gethostname(),
                ip=get_ip(socket.gethostname()),
                is_priority_node=False,
                status=task.status,
                estimated_completion_time=completion_time_to_remaining_sec(task.estimated_completion_time),
                cpu=ResourceInfo(fully_used=10, used=15, available=15, units=ResourceUnitsEnum.cores),
                ram=ResourceInfo(fully_used=115, used=115, available=120, units=ResourceUnitsEnum.gigabytes),
                gpu=None
            )
        ]
    if tunings.include_events:
        kwargs['events'] = []

    return kwargs


def batch_handle_tunings(tasks: List[TaskInfo], tunings: TaskTunings) -> Dict[str, Any]:
    pinfos, authors, nodes, events = [], [], [], []

    if tunings.include_project:
        project_uuids = [task.project_id for task in tasks]
        pinfos = get_multiple_project_infos(project_uuids)

    if tunings.include_author:
        user_uuids = [task.user_id for task in tasks]
        authors = get_multiple_user_infos(user_uuids)

    if tunings.include_nodes:
        nodes = [
            [
                NodeInfo(
                    name=socket.gethostname(),
                    ip=get_ip(socket.gethostname()),
                    is_priority_node=False,
                    status=task.status,
                    estimated_completion_time=completion_time_to_remaining_sec(task.estimated_completion_time),
                    cpu=ResourceInfo(fully_used=10, used=15, available=15, units=ResourceUnitsEnum.cores),
                    ram=ResourceInfo(fully_used=115, used=115, available=120, units=ResourceUnitsEnum.gigabytes),
                    gpu=None
                )
            ]
            for task in tasks
        ]

    if tunings.include_events:
        events = [
            []
            for task in tasks
        ]

    uuids2kwargs = {
        task.uid: {
            **(pinfo.to_args() if pinfo is not None else dict()),
            **(author.to_args() if author is not None else dict()),
            **({'nodes': node} if node is not None else dict()),
            **({'events': event} if event is not None else dict())
        }
        for task, pinfo, author, node, event in zip_longest(tasks, pinfos, authors, nodes, events)
    }

    return uuids2kwargs


def parse_job_id(job_id: str) -> tuple[JobType, str]:
    parsed = urlparse(job_id)

    job_type: JobType = parsed.scheme in set(JobType) and JobType(parsed.scheme) or JobType.default()
    scheme_str = f"{job_type.value}://"
    job_id_ = job_id.replace(scheme_str, "", 1) if job_id.startswith(scheme_str) else job_id
    return job_type, job_id_


async def handle_source(
        codegen_service: CodegenIntrospectionService,
        task: BatchTaskRequest,
        task_type: TaskType,
        token: TokenInfo
):
    """
    Handles graph and script job types. Also, passes custom ops to container env.
    For graph, compiles script and adds it to task.parameters["pyscript_source"];
    For script, simply moves it to task.parameters["pyscript_source"].
    Raises:
        JobSourceMissing, if job_source was required by job_id, but is missing;
        Other introspection and code generation related exceptions.
    """
    ops_env = "RNSEISM_CODEGEN_STEP_IMPORTER_PATH"

    job_type, clear_job_id = parse_job_id(task.job_id)
    if job_type.requires_source():
        if task.job_source is None:
            raise JobSourceMissing(f"{job_type.name} job type requires source (job_source was None).")
        if task_type != TaskType.batch:
            raise InvalidTaskType(f"Task with id '{task.job_id}' (detected '{job_type}') should be of type 'batch'."
                                  f" Was '{task_type}'.")

        introspection: CodegenIntrospection = await codegen_service.get_introspection(clear_job_id)
        task.parameters["pyscript_type"] = introspection.pipeline_type

        if task.environment is None:
            task.environment = {ENV_VAR_TOKEN: token.token}
        elif ENV_VAR_TOKEN not in task.environment:
            task.environment[ENV_VAR_TOKEN] = token.token
        else:
            logger.warning(f"Env var {ENV_VAR_TOKEN} is already passed in environment."
                           " Didn't switch to token from request.")

        db_ops = introspection.ops_lib_path
        if db_ops:
            ops = [f"file://{op}" for op in db_ops.split('\n')]
            if ops_env not in task.environment:
                task.environment[ops_env] = str(ops)
            else:
                logger.warning(f"Env var {ops_env} is already passed in environment, while having custom ops in db."
                               " Extending it.")

                existing_ops = task.environment[ops_env]
                try:
                    existing_ops_list = ast.literal_eval(existing_ops)
                except:
                    raise Exception(f"Incorrect environment {ops_env}. Should be str() of list of strings, was: \"{existing_ops}\"")
                existing_ops_list.extend(ops)
                task.environment[ops_env] = str(existing_ops_list)

        if job_type is JobType.graph:
            script = await codegen_service.compile_graph(clear_job_id, task.job_source)
            task.parameters["pyscript_source"] = script
            task.job_id = f"script://{clear_job_id}"
        elif job_type is JobType.script:
            task.parameters["pyscript_source"] = task.job_source


class ServiceException(Exception):
    pass


class ErrorToRetrieveInfoFromExternalService(ServiceException):
    pass


class DependencyNotFound(ServiceException):
    pass


class JobSourceMissing(ServiceException):
    pass


class InvalidTaskType(ServiceException):
    pass
