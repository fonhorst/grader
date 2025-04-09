import asyncio
import functools
import logging
import os
from typing import Optional

from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from grader.checking.base import CheckerReport
from grader.checking.checking import run_checking
from grader.db.tasks import TaskStatus, get_task, update_task_status_with_isolation
from grader.faststream_tasks.schemes import CheckingResult, CheckingTask

used_run_checking = run_checking

logger = logging.getLogger(__name__)

broker_queue_name = os.environ.get("GRADER_FASTSTREAM_BROKER_QUEUE", "test-queue")
broker_url = os.environ.get("GRADER_FASTSTREAM_BROKER", "amqp://admin:admin@localhost:5672/")
max_concurrency = int(os.environ.get("GRADER_FASTSTREAM_MAX_CONCURRENCY", "1"))

broker = RabbitBroker(broker_url, max_consumers=max_concurrency) 
app = FastStream(broker)

UNEXPECTED_ERROR_MESSAGE = "Unexpected error happened during the check. Contact the administrator."


async def run_check_with_cancellation(task: CheckingTask) -> CheckerReport:
    """
    Run checking function with cancellation support.
    
    Args:
        task: Task to check
        
    Returns:
        CheckerReport if successful, None if cancelled
    """
    # Run the synchronous checking function in a thread pool executor
    func = functools.partial(used_run_checking, check_type=task.check_type, **task.args)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func)


@broker.subscriber(broker_queue_name)
async def check(task: CheckingTask, msg: RabbitMessage) -> Optional[CheckingResult]:
    logger.info(f"Starting to check {task.task_uid}")

    allow_exceptions = os.environ.get("GRADER_ALLOW_EXCEPTIONS_IN_REPORT", "0") == "1"
    check_task_delay = float(os.environ.get("GRADER_CHECK_TASK_DELAY", "0.2"))

    check_task = None
    try:
        # We ensure that we starting task is not yet started or previously interrupted by some external event
        # We don't want to start the task if it is already in the FINISHED, FAILED or CANCELLED state
        attempt = await update_task_status_with_isolation(
            task_id=task.task_uid,
            expected_status=[TaskStatus.CREATED, TaskStatus.RUNNING],
            status=TaskStatus.RUNNING,
        )

        if not attempt.is_success:
            logger.warning(f"Task {task.task_uid} cannot be started "
                         f"because it is not in the expected state: {attempt.current_status}. "
                         f"Expected: {TaskStatus.CREATED}")
            return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
        
        logger.info(f"Task {task.task_uid} change its state to: {attempt.current_status}")

        # Create and start the checking task
        check_task = asyncio.create_task(run_check_with_cancellation(task))
        
        # Wait for completion or cancellation
        # TODO: verify the logic here and shield of cancellation with timeout works as expected
        while True:
            if check_task.done():
                report = check_task.result()
                logger.info(f"Report: {report}")
                break
            
            # Check if task was cancelled
            db_task = await get_task(task.task_uid)
            if db_task.is_cancelled:
                check_task.cancel()
                attempt = await update_task_status_with_isolation(
                    task_id=task.task_uid,
                    expected_status=TaskStatus.RUNNING,
                    status=TaskStatus.CANCELLED
                )
                if not attempt.is_success:
                    logger.warning(f"Task {task.task_uid} cannot be cancelled: "
                                 f"current status is {attempt.current_status}")
                logger.info(f"Task {task.task_uid} was cancelled")
                return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
        
            await asyncio.sleep(check_task_delay)

        if report is None:  # Task was cancelled
            return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
            
        logger.debug(f"Received report for {task.task_uid}: {report}")
        
        # Update status to finished with the report
        attempt = await update_task_status_with_isolation(
            task_id=task.task_uid,
            expected_status=TaskStatus.RUNNING,
            status=TaskStatus.FINISHED,
            report=report.model_dump_json()
        )

        if not attempt.is_success:
            raise ValueError(f"Task {task.task_uid} cannot be marked as finished: "
                             f"current status is {attempt.current_status}")
        
        logger.info(f"Successfully finished checking {task.task_uid}")
        return CheckingResult(task_uid=task.task_uid, report=report)
        
    except Exception as ex:
        logger.error(f"Error checking {task.task_uid}: {str(ex)}")#, exc_info=True)
        # Update status to failed with error message
        fail_reason = str(ex) if allow_exceptions else UNEXPECTED_ERROR_MESSAGE
        try:
            attempt = await update_task_status_with_isolation(
                task_id=task.task_uid,
                expected_status=TaskStatus.RUNNING,
                status=TaskStatus.FAILED,
                report=CheckerReport(checks=[], fail_reason=fail_reason).model_dump_json()
            )
            if not attempt.is_success:
                logger.warning(f"Task {task.task_uid} cannot be marked as failed: current status is {attempt.current_status}")
            
            # If we can't update the status, we should cancel the task
            if check_task and not check_task.done():
                check_task.cancel()
        except Exception as e:
            logger.error(f"Error updating task status for {task.task_uid} to FAILED: {str(e)}")#, exc_info=True)
        
        # raise FastStreamCheckTaskException("FastStream 'check' handler failed") from ex
        return None
    finally:
        # Acknowledge the message only after all operations are complete
        logger.info(f"Acknowledging message for {task.task_uid}")
        await msg.ack()
        logger.info(f"Message for {task.task_uid} acknowledged")

