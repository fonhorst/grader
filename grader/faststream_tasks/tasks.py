import logging
import os

from faststream import FastStream
from faststream.rabbit import RabbitBroker

from grader.checking.base import CheckerReport
from grader.checking.checking import run_checking
from grader.db.tasks import TaskStatus, update_task_status
from grader.faststream_tasks.schemes import CheckingResult, CheckingTask


logger = logging.getLogger(__name__)

broker = RabbitBroker(os.environ.get("GRADER_FASTSTREAM_BROKER", "amqp://admin:admin@localhost:5672/")) 

app = FastStream(broker)


@broker.subscriber("test-queue")
async def check(task: CheckingTask) -> CheckingResult:
    logger.info(f"Starting to check {task.full_name}")
    try:
        # Update status to running
        update_task_status(task.task_uid, TaskStatus.RUNNING)
        
        # Run the check
        report = run_checking(task.check_type, **task.args)

        logger.debug(f"Received report for {task.full_name}: {report}")
        
        # Update status to finished
        update_task_status(task.task_uid, TaskStatus.FINISHED)
    except Exception as e:
        logger.error(f"Error checking {task.full_name}: {str(e)}", exc_info=True)
        # Update status to failed
        update_task_status(task.task_uid, TaskStatus.FAILED)
        raise

    logger.info(f"Succesfully finished checking {task.full_name}")

    return CheckingResult(task_uid=task.task_uid, report=report)

