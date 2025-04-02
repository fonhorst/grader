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


async def run_check_with_cancellation(task: CheckingTask) -> Optional[CheckerReport]:
    """
    Run checking function with cancellation support.
    
    Args:
        task: Task to check
        
    Returns:
        CheckerReport if successful, None if cancelled
    """
    try:
        return run_checking(task.check_type, **task.args)
    except asyncio.CancelledError:
        logger.info(f"Check for task {task.task_uid} was cancelled")
        return None


@broker.subscriber("test-queue")
async def check(task: CheckingTask) -> CheckingResult:
    logger.info(f"Starting to check {task.task_uid}")
    
    # Get task with isolation to check its status
    db_task = get_task(task.task_uid)
    if db_task.status != TaskStatus.CREATED.value:
        logger.warning(f"Task {task.task_uid} is in status {db_task.status}, skipping")
        return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
    
    try:
        # Update status to running
        update_task_status_with_isolation(task.task_uid, TaskStatus.RUNNING)
        
        # Create and start the checking task
        check_task = asyncio.create_task(run_check_with_cancellation(task))
        
        # Wait for completion or cancellation
        while not check_task.done():
            # Check if task was cancelled
            db_task = get_task(task.task_uid)
            if db_task.is_cancelled:
                check_task.cancel()
                update_task_status_with_isolation(task.task_uid, TaskStatus.CANCELLED)
                logger.info(f"Task {task.task_uid} was cancelled")
                return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
            
            await asyncio.sleep(5)  # Check every 5 seconds
        
        # Get the result
        report = await check_task
        
        if report is None:  # Task was cancelled
            return CheckingResult(task_uid=task.task_uid, report=CheckerReport(checks=[]))
            
        logger.debug(f"Received report for {task.task_uid}: {report}")
        
        # Update status to finished
        update_task_status_with_isolation(task.task_uid, TaskStatus.FINISHED)
        
        logger.info(f"Successfully finished checking {task.task_uid}")
        return CheckingResult(task_uid=task.task_uid, report=report)
        
    except Exception as e:
        logger.error(f"Error checking {task.task_uid}: {str(e)}", exc_info=True)
        # Update status to failed
        update_task_status_with_isolation(task.task_uid, TaskStatus.FAILED)
        raise

