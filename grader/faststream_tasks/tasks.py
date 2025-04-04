import asyncio
import logging
import os
from typing import Optional

from faststream import FastStream
from faststream.rabbit import RabbitBroker

from grader.checking.base import CheckerReport
from grader.checking.checking import run_checking
from grader.db.tasks import TaskStatus, get_task, update_task_status_with_isolation, get_task_with_isolation
from grader.faststream_tasks.schemes import CheckingResult, CheckingTask


logger = logging.getLogger(__name__)

broker = RabbitBroker(os.environ.get("GRADER_FASTSTREAM_BROKER", "amqp://admin:admin@localhost:5672/")) 

app = FastStream(broker)


async def run_check_with_cancellation(task: CheckingTask) -> CheckerReport:
    """
    Run checking function with cancellation support.
    
    Args:
        task: Task to check
        
    Returns:
        CheckerReport if successful, None if cancelled
    """
    # Run the synchronous checking function in a thread pool executor
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        run_checking,
        task.check_type,
        **task.args
    )


# TODO: we need to add late ack here
@broker.subscriber("test-queue")
async def check(task: CheckingTask) -> CheckingResult:
    logger.info(f"Starting to check {task.task_uid}")
    
    try:
        # We ensure that we starting task is not yet started or previously interrupted by some external event
        # We don't want to start the task if it is already in the FINISHED, FAILED or CANCELLED state
        attempt = update_task_status_with_isolation(
            task_id=task.task_uid,
            expected_status=[TaskStatus.CREATED, TaskStatus.RUNNING],
            status=TaskStatus.RUNNING,
        )

        if not attempt.is_success:
            logger.warning(f"Task {task.task_uid} cannot be started "
                         f"because it is not in the expected state: {attempt.current_status}. "
                         f"Expected: {TaskStatus.CREATED}")
            return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
        
        # Create and start the checking task
        check_task = asyncio.create_task(run_check_with_cancellation(task))
        
        # Wait for completion or cancellation
        # TODO: verify the logic here and shield of cancellation with timeout works as expected
        while True:
            try:
                report = await asyncio.wait_for(asyncio.shield(check_task), timeout=5)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                pass

            if report is not None:
                break
            
            # Check if task was cancelled
            db_task = get_task(task.task_uid)
            if db_task.is_cancelled:
                check_task.cancel()
                attempt = update_task_status_with_isolation(
                    task_id=task.task_uid,
                    expected_status=TaskStatus.RUNNING,
                    status=TaskStatus.CANCELLED
                )
                if not attempt.is_success:
                    logger.warning(f"Task {task.task_uid} cannot be cancelled: "
                                 f"current status is {attempt.current_status}")
                logger.info(f"Task {task.task_uid} was cancelled")
                return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
        
        if report is None:  # Task was cancelled
            return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
            
        logger.debug(f"Received report for {task.task_uid}: {report}")
        
        # Update status to finished with the report
        attempt = update_task_status_with_isolation(
            task_id=task.task_uid,
            expected_status=TaskStatus.RUNNING,
            status=TaskStatus.FINISHED,
            report=report.model_dump_json()
        )
        if not attempt.is_success:
            logger.warning(f"Task {task.task_uid} cannot be marked as finished: "
                         f"current status is {attempt.current_status}")
        
        logger.info(f"Successfully finished checking {task.task_uid}")
        return CheckingResult(task_uid=task.task_uid, report=report)
        
    except Exception as e:
        logger.error(f"Error checking {task.task_uid}: {str(e)}", exc_info=True)
        # Update status to failed with error message
        attempt = update_task_status_with_isolation(
            task_id=task.task_uid,
            expected_status=TaskStatus.RUNNING,
            status=TaskStatus.FAILED,
            report=CheckerReport(checks=[], fail_reason=str(e)).model_dump_json()
        )
        if not attempt.is_success:
            logger.warning(f"Task {task.task_uid} cannot be marked as failed: "
                         f"current status is {attempt.current_status}")
        raise

