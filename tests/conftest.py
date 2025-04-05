import asyncio
from typing import Optional
import pytest
import logging

from grader.db.tasks import TaskStatus, create_tables, delete_all_tasks, list_tasks
from grader.services.checker import CheckerService, TaskResponse


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def configure_sqlalchemy_logging():
    """Configure SQLAlchemy logging to WARN level to reduce test output noise."""
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    # You can add other loggers that need to be silenced here
    return None


@pytest.fixture(scope="function")
def clean_tasks_table():
    """Clear all tasks from the database before each test and return count of remaining tasks."""
    create_tables()
    delete_all_tasks()
    # Return the count of tasks after cleaning (should be 0)
    remaining_tasks = len(list_tasks())
    yield remaining_tasks
    # Cleanup after test as well
    delete_all_tasks()


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