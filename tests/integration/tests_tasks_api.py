import datetime
import logging
import os
import random
import uuid
from copy import copy
from typing import List

import pytest
from rnseism_sdk.db.tasks import TaskType
from rnseism_sdk.runner.base import FailedJobException
from rnseism_sdk.worker.utils import TestSDKJob

from geowsm.api.schemas.iworkers import IWorkerInfo, WorkerStatus
from geowsm.api.schemas.tasks import TaskInfoResponse, TaskStatus, TaskResultResponse, TaskLogResponse
from tests.utils import make_request, AppContext, wait_for_workers, wait_for_tasks, \
    wait_for_desired_num_of_active_workers, FailedRequestError, only_for_k8s

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# docker ps -a -f 'label=type=main_app' | tail -n +2 | awk '{print $1}' | xargs -L 1 docker logs
# docker ps -a -f 'label=type=batch_worker'
DEFAULT_TIMEOUT = 30

# 'remote', 'docker' or 'k8s'
APP_CONTEXT = [el.lower().strip() for el in os.environ.get("GEOWSM_PYTEST_APP_CONTEXT", "docker").split(',')]

STORAGE_JOBS_AND_PARAMETERS = [
        ("ReadVolumesAndReturnValueJob", {'uuid': "ddbcb4ce-047c-43f2-a131-728e6b4915dd"}),
        ("ExternalClientsWriteVolumeJob", {'trace_size': 100})
]

PARAMS = [(ctx, job, kwargs) for ctx in APP_CONTEXT for job, kwargs in STORAGE_JOBS_AND_PARAMETERS]


@pytest.fixture(scope='function')
def app_context(request) -> AppContext:
    if request.param == 'docker':
        return request.getfixturevalue('docker_app_context')
    elif request.param == 'k8s':
        return request.getfixturevalue('k8s_app_context')
    elif request.param == 'remote':
        return request.getfixturevalue('remote_app_context')
    else:
        raise ValueError(f'Unsupported manager type: {request.param}')


@pytest.mark.parametrize('app_context', APP_CONTEXT, indirect=True)
def test_simple_list(app_context):
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {},
            "tunings": {},
            "navigation": {}
        },
    }
    make_request(app_context, payload)


@pytest.mark.parametrize('app_context', APP_CONTEXT, indirect=True)
def test_execute_interactive_task(app_context):
    job_id = TestSDKJob.__name__
    session_uid = str(uuid.uuid4())
    param_result_keys = {
        'a': [124, 345, 67, 42.0],
        'b': 42.0,
        'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
        'd': False
    }
    param_state_keys = {
        "add": 1
    }
    workers_num = 2
    tasks_num = 3

    # create iworker service
    workers = []
    for i in range(workers_num):
        logger.info(f"CREATING worker ${i}")
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "IWorkers.create",
            "params": {
                "worker": {
                    "name": f"interpretation-server-replica-{i}",
                    "worker_type": "docker-container",
                    "image": app_context.worker_image,
                    "cpu": 1,
                    "mem": 4096
                }
            }
        }

        response = make_request(app_context, payload)
        worker = IWorkerInfo.parse_obj(response)

        assert worker.status in [WorkerStatus.created, WorkerStatus.running]

        workers.append(worker)

    # ensure that all workers are running
    wait_for_workers(app_context, uids=[w.uuid for w in workers], desired_statuses=[WorkerStatus.running])

    # check that listing is working
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

    response = make_request(app_context, payload)
    workers = [IWorkerInfo.parse_obj(el) for el in response]
    assert len(workers) == workers_num
    for worker in workers:
        assert worker.status == WorkerStatus.running

    before_the_first_task = datetime.datetime.utcnow()

    # start interactive  with a session
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.start",
        "params": {
            "task": {
                "name": "stateless-interactive-task-session-0",
                "task_type": TaskType.interactive.value,
                "job_id": job_id,
                "priority": 0.0,
                "parameters": {
                    'param_result_keys': param_result_keys,
                    'param_state_keys':  param_state_keys,
                    'duration': 2
                },
                "session_uid": session_uid
            }
        },
    }

    response = make_request(app_context, payload)
    task = TaskInfoResponse.parse_obj(response)

    assert task.uuid is not None
    logger.info(f"Got task with uid: {task.uuid}")
    assert task.status in [TaskStatus.created, TaskStatus.running]

    tasks = wait_for_tasks(app_context, uids=[task.uuid],
                           desired_statuses=[TaskStatus.finished], timeout=DEFAULT_TIMEOUT)
    task = tasks[0]

    after_the_first_task = datetime.datetime.utcnow()

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get_result",
        "params": {
            "uid": task.uuid
        },
    }

    response = make_request(app_context, payload)
    task_result = TaskResultResponse.parse_obj(response)

    assert 'task_out' in task_result.results
    assert 'hostname' in task_result.results['task_out']
    session_hostname = task_result.results['task_out']['hostname']
    assert session_hostname is not None and len(session_hostname) > 0
    results = copy(task_result.results)
    del results['task_out']
    assert results == param_result_keys

    # now executing multiple tasks
    session_tasks = [
        {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "Task.start",
            "params": {
                "task":{
                    "name": f"stateless-interactive-task-session-{i + 1}",
                    "task_type": TaskType.interactive.value,
                    "job_id": job_id,
                    "priority": 0.0,
                    "parameters": {
                        'param_result_keys': param_result_keys,
                        'param_state_keys': param_state_keys,
                        'duration': 2
                    },
                    "session_uid": session_uid
                }
            }
        }
        for i in range(tasks_num)
    ]

    not_session_tasks = [
        {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "Task.start",
            "params": {
                "task": {
                    "name": f"stateless-interactive-task-{i}",
                    "task_type": TaskType.interactive.value,
                    "job_id": job_id,
                    "priority": 0.0,
                    "parameters": {
                        'param_result_keys': param_result_keys,
                        'duration': 2
                    },
                    "session_uid": None
                }
            },
        }
        # for i in range(0)
        for i in range(tasks_num)
    ]

    tasks = [*session_tasks, *not_session_tasks]
    random.shuffle(tasks)

    task_infos = []
    for payload in tasks:
        response = make_request(app_context, payload)
        task = TaskInfoResponse.parse_obj(response)
        task_infos.append((task, payload['params']['task']['session_uid']))

    wait_for_tasks(app_context, uids=[t.uuid for t,_ in task_infos],
                   desired_statuses=[TaskStatus.finished], timeout=DEFAULT_TIMEOUT)

    task_results = []
    for task, ses_uid in task_infos:
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "Task.get_result",
            "params": {
                "uid": task.uuid
            },
        }

        response = make_request(app_context, payload)
        task_result = TaskResultResponse.parse_obj(response)
        task_results.append(task_result)

    assert all([
        'hostname' in tr.results['task_out']
        for tr, (tinfo, ses_uid) in zip(task_results, task_infos) if ses_uid is not None
    ])

    hostnames = [
        tr.results['task_out']['hostname']
        for tr, (tinfo, ses_uid) in zip(task_results, task_infos) if ses_uid is not None
    ]

    unique_hostnames_for_session_tasks = {*hostnames, session_hostname}
    assert len(unique_hostnames_for_session_tasks) == 1

    # # Perform downscaling
    active_w_id = workers[-1].uuid
    for w in workers[:-1]:
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "IWorkers.delete",
            "params": {
                "uid": w.uuid
            },
        }

        make_request(app_context, payload)

    wait_for_desired_num_of_active_workers(app_context, desired_num_of_active_workers=1)

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "IWorkers.get",
        "params": {
            "uid": active_w_id
        },
    }

    response = make_request(app_context, payload)
    worker = IWorkerInfo.parse_obj(response)

    assert worker.status == WorkerStatus.running
    assert worker.uuid == active_w_id

    # Checking a task can successfuly finish after the downscaling
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.start",
        "params": {
            "task": {
                "name": f"stateless-interactive-task-checking-finished",
                "task_type": TaskType.interactive.value,
                "job_id": job_id,
                "priority": 0.0,
                "parameters": {
                    'param_result_keys': param_result_keys,
                    'duration': 2
                },
                "session_uid": None
            }
        },
    }

    response = make_request(app_context, payload)
    task = TaskInfoResponse.parse_obj(response)

    assert task.status in [TaskStatus.created, TaskStatus.running]

    wait_for_tasks(app_context, uids=[task.uuid],
                   desired_statuses=[TaskStatus.finished], timeout=DEFAULT_TIMEOUT)

    # Check if can interrupt the task
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.start",
        "params": {
            "task":{
                "name": f"stateless-interactive-task-session-{tasks_num + 1}",
                "task_type": TaskType.interactive.value,
                "job_id": job_id,
                "priority": 0.0,
                "parameters": {
                    'param_result_keys': param_result_keys,
                    'duration': 10
                },
                "session_uid": None
            }
        },
    }

    response = make_request(app_context, payload)
    task = TaskInfoResponse.parse_obj(response)

    assert task.status in [TaskStatus.created, TaskStatus.running]

    wait_for_tasks(app_context, uids=[task.uuid],
                   desired_statuses=[TaskStatus.running], timeout=DEFAULT_TIMEOUT)

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.cancel",
        "params": {
            "uid": task.uuid
        },
    }
    make_request(app_context, payload)
    # TODO: cancel is not fully correct right now. Sometimes the task may still finish.
    #  Correct the implementation and remove 'finished' status from here
    wait_for_tasks(app_context, uids=[task.uuid],
                   desired_statuses=[TaskStatus.cancelled, TaskStatus.finished], timeout=DEFAULT_TIMEOUT)

    # Check if the task can fail
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.start",
        "params": {
            "task": {
                "name": f"stateless-interactive-task-session-{tasks_num + 2}",
                "task_type": TaskType.interactive.value,
                "job_id": job_id,
                "priority": 0.0,
                "parameters": {
                    'param_result_keys': param_result_keys,
                    'param_do_fail': True,
                    'duration': 2
                },
                "session_uid": None
            }
        },
    }

    response = make_request(app_context, payload)
    task = TaskInfoResponse.parse_obj(response)

    assert task.status in [TaskStatus.created, TaskStatus.running]

    wait_for_tasks(app_context, uids=[task.uuid], desired_statuses=[TaskStatus.failed], timeout=DEFAULT_TIMEOUT)

    # checking reason and logs for the failed task
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get",
        "params": {
            "uid": task.uuid,
            "tunings": {
                "includeReason": True
            }
        }
    }
    failed_task = TaskInfoResponse.parse_obj(make_request(app_context, payload))
    assert failed_task.reason is not None and len(failed_task.reason) > 0
    assert FailedJobException.__name__ in failed_task.reason

    # TODO: replace with reading iworker logs
    # payload = {
    #     "jsonrpc": "2.0",
    #     "id": 0,
    #     "method": "Task.get_log",
    #     "params": {
    #         "uid": task.uuid
    #     }
    # }
    # response = make_request(main_app, payload)
    # log = TaskLogResponse.parse_obj(response)
    # assert log.log is not None and len(log.log) > 0
    # assert "Raising exception" in log.log

    # Checking listing of tasks
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {},
            "tunings": {},
            "navigation": {}
        },
    }
    response = make_request(app_context, payload)
    tasks = [TaskInfoResponse.parse_obj(r) for r in response]

    finished_tasks = [t for t in tasks if t.status == TaskStatus.finished]
    failed_tasks = [t for t in tasks if t.status == TaskStatus.failed]
    cancelled_tasks = [t for t in tasks if t.status == TaskStatus.cancelled]

    assert len(tasks) == tasks_num * 2 + 4
    assert len(failed_tasks) == 1
    assert len(cancelled_tasks) == 1
    assert len(finished_tasks) == len(tasks) - 2

    # advanced listing request №1
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {
                "project_id": {"$in": [app_context.project_id]},
                "user_uid": {"$eq": app_context.user_id},
                "task_type": {"$in": [TaskType.interactive.value, TaskType.batch.value]},
                "status": {"$in": [TaskStatus.failed.value, TaskStatus.cancelled.value]}
            },
            "tunings": {
                "includeAuthor": "true",
                "includeProject": "true",
                "includeNodes": "true",
                "includeEvents": "true"
            },
            "navigation": {}
        },
    }

    response = make_request(app_context, payload)
    tasks = [TaskInfoResponse.parse_obj(r) for r in response]

    finished_tasks = [t for t in tasks if t.status == TaskStatus.finished]
    failed_tasks = [t for t in tasks if t.status == TaskStatus.failed]
    cancelled_tasks = [t for t in tasks if t.status == TaskStatus.cancelled]

    assert len(failed_tasks) == 1
    assert len(cancelled_tasks) == 1
    assert len(finished_tasks) == 0
    assert all(task.author_name is not None and len(task.author_name) > 0 for task in tasks)
    assert all(task.project_name is not None and len(task.project_name) > 0 for task in tasks)
    assert all(task.project_info is not None and len(task.project_info) > 0 for task in tasks)
    assert all(task.nodes is not None and len(task.nodes) > 0 for task in tasks)
    assert all(task.events is not None for task in tasks)

    # advanced listing request №2
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {
                "project_id": {"$in": [app_context.project_id]},
                "user_uid": {"$eq": app_context.user_id},
                "task_type": {"$in": [TaskType.interactive.value, TaskType.batch.value]},
                "status": {"$in": [TaskStatus.finished.value]}
            },
            "tunings": {
                "includeAuthor": "true",
                "includeProject": "true",
                "includeNodes": "true",
                "includeEvents": "true"
            },
            "navigation": {}
        },
    }

    response = make_request(app_context, payload)
    tasks = [TaskInfoResponse.parse_obj(r) for r in response]

    finished_tasks = [t for t in tasks if t.status == TaskStatus.finished]
    failed_tasks = [t for t in tasks if t.status == TaskStatus.failed]
    cancelled_tasks = [t for t in tasks if t.status == TaskStatus.cancelled]

    assert len(failed_tasks) == 0
    assert len(cancelled_tasks) == 0
    assert len(finished_tasks) == tasks_num * 2 + 2
    assert all(task.author_name is not None and len(task.author_name) > 0 for task in tasks)
    assert all(task.project_name is not None and len(task.project_name) > 0 for task in tasks)
    assert all(task.project_info is not None and len(task.project_info) > 0 for task in tasks)
    assert all(task.nodes is not None and len(task.nodes) > 0 for task in tasks)
    assert all(task.events is not None for task in tasks)

    # advanced listing request №3
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {
                "uuid": {"$in": [f"interactive--{uuid.uuid4()}", *(task.uuid for task in finished_tasks)]}
            },
            "tunings": {
                "includeAuthor": "true",
                "includeProject": "true",
                "includeNodes": "true",
                "includeEvents": "true"
            },
            "navigation": {}
        },
    }

    response = make_request(app_context, payload)
    tasks = [TaskInfoResponse.parse_obj(r) for r in response]
    assert len(tasks) == len(finished_tasks)
    # assert len([t.uuid for t in tasks]) == len([t.uuid for t in finished_tasks])

    # advanced listing request №4
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {
                "submit_time": {"$gte": before_the_first_task.isoformat(), "$lte": after_the_first_task.isoformat()}
            },
            "tunings": {
                "includeAuthor": "true",
                "includeProject": "true",
                "includeNodes": "true",
                "includeEvents": "true"
            },
            "navigation": {}
        },
    }

    response = make_request(app_context, payload)
    tasks = [TaskInfoResponse.parse_obj(r) for r in response]
    assert len(tasks) == 1

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {
                "submit_time": {"$gte": after_the_first_task.isoformat()}
            },
            "tunings": {
                "includeAuthor": "true",
                "includeProject": "true",
                "includeNodes": "true",
                "includeEvents": "true"
            },
            "navigation": {}
        },
    }

    response = make_request(app_context, payload)
    tasks = [TaskInfoResponse.parse_obj(r) for r in response]
    # finished + failed + cancelled
    assert len(tasks) == (len(finished_tasks) + 1 + 1) - 1

    # advanced listing request №5
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {
                "name": {"$in": [f"stateless-interactive-task-{i}" for i in range(tasks_num)]}
            },
            "tunings": {
                "includeAuthor": "true",
                "includeProject": "true",
                "includeNodes": "true",
                "includeEvents": "true"
            },
            "navigation": {}
        },
    }

    response = make_request(app_context, payload)
    tasks = [TaskInfoResponse.parse_obj(r) for r in response]
    assert len(tasks) == tasks_num

    # advanced listing request №6
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.list",
        "params": {
            "filter": {
                "name": {"$eq": "stateless-interactive-task-sess%"}
            },
            "tunings": {
                "includeAuthor": "true",
                "includeProject": "true",
                "includeNodes": "true",
                "includeEvents": "true"
            },
            "navigation": {}
        },
    }

    response = make_request(app_context, payload)
    tasks = [TaskInfoResponse.parse_obj(r) for r in response]
    assert len(tasks) == tasks_num + 3

    # checking tunings in get method
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get",
        "params": {
            "uid": tasks[0].uuid,
            "tunings": {
                "includeAuthor": "true",
                "includeProject": "true",
                "includeNodes": "true",
                "includeEvents": "true"
            }
        },
    }

    response = make_request(app_context, payload)
    task = TaskInfoResponse.parse_obj(response)

    assert task.author_name is not None and len(task.author_name) > 0
    assert task.project_name is not None and len(task.project_name) > 0
    assert task.project_info is not None and len(task.project_info) > 0
    assert task.nodes is not None and len(task.nodes) > 0
    assert task.events is not None


@pytest.mark.parametrize('app_context', APP_CONTEXT, indirect=True)
def test_execute_batch_task(app_context):
    job_id = TestSDKJob.__name__

    def create_task(i: int, duration: float = 5.0, do_fail: bool = False) -> TaskInfoResponse:
        param_result_keys = {
            'a': [124, 345, 67, 42.0],
            'b': 42.0,
            'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
            'd': False
        }

        task_payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "Task.start",
            "params": {
                "task": {
                    "name": f"batch-task-{i}",
                    "task_type": TaskType.batch.value,
                    "job_id": job_id,
                    "priority": 0.0,
                    "parameters": {
                        'duration': duration,
                        'param_result_keys': param_result_keys,
                        'param_do_fail': do_fail
                    },
                    "environment": {},
                    "image": app_context.batch_task_image
                }
            },
        }

        task_resp = TaskInfoResponse.parse_obj(make_request(app_context, task_payload))

        assert task_resp.status in [TaskStatus.created, TaskStatus.running]

        return task_resp

    # ensure task ca be finished successfully
    task = create_task(i=0, duration=2)
    wait_for_tasks(app_context, uids=[task.uuid],
                   desired_statuses=[TaskStatus.finished], timeout=DEFAULT_TIMEOUT)

    # checking logs availability
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get_log",
        "params": {
            "uid": task.uuid
        }
    }
    response = make_request(app_context, payload)
    log = TaskLogResponse.parse_obj(response)
    assert log.log is not None and len(log.log) > 0
    assert f"Start of job {job_id}" in log.log

    # ensure task can be cancelled
    task = create_task(i=1, duration=10)

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.cancel",
        "params": {
            "uid": task.uuid
        }
    }

    make_request(app_context, payload)

    wait_for_tasks(app_context, uids=[task.uuid],
                   desired_statuses=[TaskStatus.cancelled], timeout=DEFAULT_TIMEOUT)

    # ensure task can be failed
    task = create_task(i=2, do_fail=True)
    wait_for_tasks(app_context, uids=[task.uuid],
                   desired_statuses=[TaskStatus.failed], timeout=DEFAULT_TIMEOUT)

    # checking reason and logs for the failed task
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get",
        "params": {
            "uid": task.uuid,
            "tunings":{
                "includeReason": True
            }
        }
    }
    failed_task = TaskInfoResponse.parse_obj(make_request(app_context, payload))
    assert failed_task.reason is not None and len(failed_task.reason) > 0
    assert FailedJobException.__name__ in failed_task.reason

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get_log",
        "params": {
            "uid": task.uuid
        }
    }
    response = make_request(app_context, payload)
    log = TaskLogResponse.parse_obj(response)
    assert log.log is not None and len(log.log) > 0
    assert "Raising exception" in log.log


@pytest.mark.parametrize('app_context', APP_CONTEXT, indirect=True)
def test_batch_and_interactive_run_method(app_context):
    job_id = TestSDKJob.__name__
    param_result_keys = {
        'a': [124, 345, 67, 42.0],
        'b': 42.0,
        'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
        'd': False
    }

    # create iworker service
    logger.info(f"CREATING intercative worker")
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "IWorkers.create",
        "params": {
            "worker": {
                "name": f"interpretation-server-replica-0",
                "worker_type": "docker-container",
                "image": app_context.worker_image,
                "cpu": 1,
                "mem": 4096
            }
        }
    }

    response = make_request(app_context, payload)
    worker = IWorkerInfo.parse_obj(response)

    assert worker.status in [WorkerStatus.created, WorkerStatus.running]

    # ensure that all workers are running
    wait_for_workers(app_context, uids=[worker.uuid], desired_statuses=[WorkerStatus.running])

    # run task
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.run",
        "params": {
            "task": {
                "name": "stateless-interactive-task-0",
                "task_type": TaskType.interactive.value,
                "job_id": job_id,
                "priority": 0.0,
                "time_limit": 60.0,
                "parameters": {
                    'param_result_keys': param_result_keys,
                    'duration': 2
                },
                "session_uid": None
            }
        },
    }

    response = make_request(app_context, payload)
    task_result = TaskResultResponse.parse_obj(response)

    assert 'task_out' in task_result.results
    assert 'hostname' in task_result.results['task_out']
    session_hostname = task_result.results['task_out']['hostname']
    assert session_hostname is not None and len(session_hostname) > 0
    results = copy(task_result.results)
    del results['task_out']
    assert results == param_result_keys

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.run",
        "params": {
            "task": {
                "name": f"batch-task-0",
                "task_type": TaskType.batch.value,
                "job_id": job_id,
                "priority": 0.0,
                "time_limit": 60.0,
                "parameters": {
                    'duration': 2,
                    'param_result_keys': param_result_keys
                },
                "environment": {},
                "image": app_context.batch_task_image
            }
        },
    }

    response = make_request(app_context, payload)
    task_result = TaskResultResponse.parse_obj(response)

    assert 'task_out' in task_result.results
    assert 'hostname' in task_result.results['task_out']
    results = copy(task_result.results)
    del results['task_out']
    assert results == param_result_keys


@pytest.mark.skip("Does not work without accessible storage. Remove the mark if the storage available")
@pytest.mark.parametrize('app_context,job_id,parameters', PARAMS, indirect=["app_context"])
def test_execute_dsl_based_interactive_task(app_context, job_id, parameters):
    ######## CREATE WORKER ######################################
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "IWorkers.list",
        "params": {
            "filter": {},
            "tunings": {},
            "navigation": {}
        }
    }
    response = make_request(app_context, payload)
    iwinfos: List[IWorkerInfo] = [IWorkerInfo.parse_obj(el) for el in response]
    if not any([iwinfo.project_id == app_context.project_id for iwinfo in iwinfos]):
        # create worker
        logger.info(f"CREATING worker 0")
        payload = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "IWorkers.create",
            "params": {
                "worker": {
                    "name": f"interpretation-server-replica-0",
                    "worker_type": "docker-container",
                    "image": app_context.worker_image,
                    "cpu": 1,
                    "mem": 4096
                }
            }
        }

        response = make_request(app_context, payload)
        worker: IWorkerInfo = IWorkerInfo.parse_obj(response)

        assert worker.status in [WorkerStatus.created, WorkerStatus.running]

        # ensure that all workers are running
        wait_for_workers(app_context, uids=[worker.uuid], desired_statuses=[WorkerStatus.running])

    ######## START TASK ######################################
    # start interactive  with a session
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.start",
        "params": {
            "task": {
                "name": "stateless-interactive-task-session-0",
                "task_type": TaskType.interactive.value,
                "job_id": job_id,
                "priority": 0.0,
                "parameters": parameters,
                "session_uid": None
            }
        },
    }

    response = make_request(app_context, payload)
    task = TaskInfoResponse.parse_obj(response)

    assert task.uuid is not None
    logger.info(f"Got task with uid: {task.uuid}")
    assert task.status in [TaskStatus.created, TaskStatus.running]

    tasks = wait_for_tasks(app_context, uids=[task.uuid],
                           desired_statuses=[TaskStatus.finished], timeout=2 * DEFAULT_TIMEOUT)
    task = tasks[0]

    ######## GET TASK RESULT ######################################
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get_result",
        "params": {
            "uid": task.uuid
        },
    }

    response = make_request(app_context, payload)
    task_result = TaskResultResponse.parse_obj(response)

    assert 'task_out' in task_result.results
    logger.info(f"Task out: {task_result.results['task_out']}")


@pytest.mark.skip("Does not work without accessible storage. Remove the mark if the storage available")
@pytest.mark.parametrize('app_context,job_id,parameters', PARAMS, indirect=["app_context"])
def test_execute_dsl_batch_task(app_context, job_id, parameters):
    ######## START TASK ######################################
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.start",
        "params": {
            "task": {
                "name": f"batch-task-0",
                "task_type": TaskType.batch.value,
                "job_id": job_id,
                "priority": 0.0,
                "parameters": parameters,
                "environment": {},
                "image": app_context.batch_task_image
            }
        },
    }

    response = make_request(app_context, payload)
    task = TaskInfoResponse.parse_obj(response)

    assert task.status in [TaskStatus.created, TaskStatus.running]

    wait_for_tasks(app_context, uids=[task.uuid],
                   desired_statuses=[TaskStatus.finished], timeout=2 * DEFAULT_TIMEOUT)

    ######## GET TASK RESULT ######################################
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get_result",
        "params": {
            "uid": task.uuid
        },
    }

    response = make_request(app_context, payload)
    task_result = TaskResultResponse.parse_obj(response)

    assert 'task_out' in task_result.results
    logger.info(f"Task out: {task_result.results['task_out']}")


@pytest.mark.parametrize('app_context', APP_CONTEXT, indirect=True)
def test_interactive_task_no_workers(app_context, auth_token_2):
    job_id = TestSDKJob.__name__
    session_uid = str(uuid.uuid4())
    workers = []
    param_result_keys = {
        'a': [124, 345, 67, 42.0],
        'b': 42.0,
        'c': {'sub_c_1': 'aaa', 'sub_c_2': 'bbbb'},
        'd': False
    }
    param_state_keys = {
        "add": 1
    }

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "IWorkers.create",
        "params": {
            "worker": {
                "name": f"interpretation-server-replica",
                "worker_type": "docker-container",
                "image": app_context.worker_image,
                "cpu": 1,
                "mem": 4096
            }
        }
    }

    response = make_request(app_context, payload)
    worker = IWorkerInfo.parse_obj(response)

    assert worker.status in [WorkerStatus.created, WorkerStatus.running]

    workers.append(worker)

    wait_for_workers(app_context, uids=[w.uuid for w in workers], desired_statuses=[WorkerStatus.running])

    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.start",
        "params": {
            "task": {
                "name": "stateless-interactive-task-session-0",
                "task_type": TaskType.interactive.value,
                "job_id": job_id,
                "priority": 0.0,
                "parameters": {
                    'param_result_keys': param_result_keys,
                    'param_state_keys': param_state_keys,
                    'duration': 2
                },
                "session_uid": session_uid
            }
        },
    }
    logger.info(f"Making request with another project_id")
    with pytest.raises(FailedRequestError):
        make_request(app_context, payload, auth_token_2)


@pytest.mark.parametrize('app_context', only_for_k8s(APP_CONTEXT), indirect=True)
def test_execute_spark_batch_task(app_context):
    job_id = "SparkTestCustomOpsJob"  # "SparkTestJob"
    # job_id = "SparkTestJob"

    task_payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.start",
        "params": {
            "task": {
                "name": "spark-task",
                "task_type": "spark",
                "job_id": job_id,
                "priority": 0.0,
                "environment": {
                    "RNSEISM_CODEGEN_STEP_IMPORTER_PATH": "['file:///usr/local/lib/python3.10/site-packages/rnseism_sdk/spark/custom/custom_processors.py']"
                },
                "parameters": dict(),
                "image": app_context.batch_task_image,
                "service_type": "NodePort"
            }
        }
    }

    task = TaskInfoResponse.parse_obj(make_request(app_context, task_payload))

    assert task.status in [TaskStatus.created, TaskStatus.running]

    # ensure task ca be finished successfully
    wait_for_tasks(app_context, uids=[task.uuid],
                   desired_statuses=[TaskStatus.finished], timeout=DEFAULT_TIMEOUT)

    # checking logs availability
    payload = {
        "jsonrpc": "2.0",
        "id": 0,
        "method": "Task.get_result",
        "params": {
            "uid": task.uuid
        }
    }
    response = make_request(app_context, payload)
    task_result = TaskResultResponse.parse_obj(response)
    assert 'task_out' in task_result.results
    assert 'output_uuids' in task_result.results['task_out']
    assert len(task_result.results['task_out']['output_uuids']) > 0
