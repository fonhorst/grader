import logging
import os
from typing import Any, Dict

from faststream import FastStream
from faststream.rabbit import RabbitBroker

from grader.checking.base import CheckerReport
from grader.checking.checking import run_checking
from grader.faststream_tasks.schemes import CheckingResult, CheckingTask


logger = logging.getLogger(__name__)

broker = RabbitBroker(os.environ.get("GRADER_FASTSTREAM_BROKER", "amqp://admin:admin@localhost:5672/")) 

app = FastStream(broker)


@broker.subscriber("test-queue")
async def check(task: CheckingTask) -> CheckingResult:
    print("JUST A TEST")

    return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))

    report = run_checking(task.check_type, **task.args)

    return CheckingResult(task_uid=task.task_uid, report=report)

