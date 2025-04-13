import os
import asyncio
import aio_pika
from typing import Optional, List, Union
import pytest
import logging

import pytest_asyncio

from grader.faststream_tasks.tasks import broker_url, broker_queue_name
from grader.db.tasks import TaskStatus, create_tables, delete_all_tasks, drop_all_tables, list_tasks
from grader.services.checker import CheckerService, TaskInfo


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def configure_sqlalchemy_logging():
    """Configure SQLAlchemy logging to WARN level to reduce test output noise."""
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    # You can add other loggers that need to be silenced here
    return None


@pytest.fixture(scope="session")
def broker_queue_name():
    """Return the broker queue name used in FastStream tasks."""
    return "test-queue"


# TODO: add cleaning of rabbitmq test-queue


@pytest_asyncio.fixture(scope="function")
async def clean_tasks_table():
    """Clear all tasks from the database before each test and return count of remaining tasks."""
    await create_tables()
    await drop_all_tables()
    # Return the count of tasks after cleaning (should be 0)
    tasks = await list_tasks()
    remaining_tasks = len(tasks)
    yield remaining_tasks
    # Cleanup after test as well
    await drop_all_tables()


@pytest_asyncio.fixture(scope="function")
async def clean_rabbitmq_queue(broker_queue_name):
    """
    Clean RabbitMQ queue by deleting and recreating it before and after a test function.
    
    This fixture ensures that each test starts with an empty queue and also cleans up
    after itself to prevent any messages from affecting subsequent tests.
    """

    logger.info(f"Cleaning RabbitMQ queue '{broker_queue_name}' before test")
    
    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust(broker_url)
    
    try:
        # Create channel
        channel = await connection.channel()
        
        # Delete the queue if it exists
        await channel.queue_delete(broker_queue_name, timeout=2)
        logger.info(f"Deleted RabbitMQ queue '{broker_queue_name}' before test")      
        
        # Run the test
        yield broker_queue_name
        
        # Delete the queue after the test
        await channel.queue_delete(broker_queue_name, timeout=2)
        logger.info(f"Deleted RabbitMQ queue '{broker_queue_name}' after test")
        
    finally:
        # Close the connection
        await connection.close()


async def wait_for_status(
    service: CheckerService,
    task_id: str,
    expected_status: TaskStatus,
    timeout: float = 5.0,
    check_interval: float = 0.1
) -> Optional[TaskInfo]:
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
        status = await service.status(task_id)
        current_status = status.status
        logger.debug(f"Obtained status: {current_status}")
        if current_status == expected_status.value:
            return status
            
        # Wait before next check
        await asyncio.sleep(check_interval)

 