import asyncio
from typing import Optional

from grader.db.tasks import TaskStatus
from grader.services.checker import CheckerService, TaskResponse
import logging


logger = logging.getLogger(__name__)


async def wait_for_status(
    service: CheckerService,
    task_id: str,
    expected_status: TaskStatus,
    timeout: float = 5.0,
    check_interval: float = 0.1
) -> Optional[TaskResponse]:
    """
    Wait for a task to reach a specific status.
    
    Args:
        service: CheckerService instance
        task_id: ID of the task to monitor
        expected_status: Status to wait for
        timeout: Maximum time to wait in seconds
        check_interval: Time between status checks in seconds
        
    Returns:
        TaskResponse if status is reached, None if timeout
    """
    start_time = asyncio.get_event_loop().time()
    
    current_status = None
    while True:
        # Check if timeout reached
        if asyncio.get_event_loop().time() - start_time > timeout:
            logger.warning(f"Task {task_id} timed out after {timeout} seconds. Last status: {current_status} while waiting for {expected_status}")
            return None
            
        # Get current status
        status = service.status(task_id)
        current_status = status.status
        if current_status == expected_status.value:
            return status
            
        # Wait before next check
        await asyncio.sleep(check_interval) 