from time import sleep
import pytest
import asyncio
from faststream.rabbit import TestRabbitBroker

from grader.checking.base import CheckReport
from grader.checking.checking import CheckType, CheckerReport
from grader.db.tasks import TaskStatus
from grader.faststream_tasks.schemes import FastStreamCheckTaskException
from grader.faststream_tasks.tasks import UNEXPECTED_ERROR_MESSAGE, broker, check
from grader.services.checker import CheckerService, TaskResponse
from tests.conftest import wait_for_status
import logging

logger = logging.getLogger(__name__)


# TODO: remove it later
@pytest.mark.asyncio
async def test_empty():
    logger.error("Just an empty test")


@pytest.mark.asyncio
async def test_task_submit_positive(clean_tasks_table, clean_rabbitmq_queue, monkeypatch):
    """Test complete task lifecycle with successful execution."""

    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(broker=br)
        
        # Test data
        user_id = "test_user"
        check_type = CheckType.CLICKHOUSE
        args = {"host": "localhost", "user": "admin", "password": "admin"}
        name = "Test Task"
        tag = "test_tag"
        
        # Mock the checking function to return a successful report after delay
        mock_report = CheckerReport(checks=[
            CheckReport(required=True, passed=True, check_description="Test check")
        ])
        
        def mock_checking(*args, **kwargs):
            sleep(0.5)  # Simulate work
            logger.debug(f"Mock checking function called: Args={args}, Kwargs={kwargs}")
            return mock_report
        
        with monkeypatch.context() as m:
            m.setattr('grader.faststream_tasks.tasks.used_run_checking', mock_checking)
            
            # 1. Submit task
            response = await service.submit(
                user_id=user_id,
                check_type=check_type,
                args=args,
                name=name,
                tag=tag
            )
            
            # Verify initial state
            assert isinstance(response, TaskResponse)
            assert response.status == TaskStatus.CREATED.value
            
            # 2. Wait for task to start running
            task_id = response.id
            running_status = await wait_for_status(service, task_id, TaskStatus.RUNNING)
            assert running_status is not None, "Task did not reach RUNNING state"
            
            # 3. Wait for task to finish
            finished_status = await wait_for_status(service, task_id, TaskStatus.FINISHED)
            assert finished_status is not None, "Task did not reach FINISHED state"
            assert finished_status.report is not None
            
            # 4. Parse and verify report content
            report = CheckerReport.model_validate_json(finished_status.report)
            assert len(report.checks) == 1
            assert report.checks[0].check_description == "Test check"
            assert report.checks[0].passed is True
            assert report.checks[0].required is True


@pytest.mark.asyncio
async def test_task_submit_negative(clean_tasks_table, clean_rabbitmq_queue, monkeypatch):
    """Test complete task lifecycle with failed execution."""

    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(broker=br)
        
        # Test data
        user_id = "test_user"
        check_type = CheckType.CLICKHOUSE
        args = {"host": "localhost", "user": "admin", "password": "admin"}
        name = "Test Task"
        tag = "test_tag"
        
        # Mock the checking function to raise an exception after delay
        test_error = ValueError("Test error in checking function")
        
        def mock_checking(*args, **kwargs):
            sleep(0.5)  # Simulate work
            raise test_error
        
        with monkeypatch.context() as m:
            m.setattr('grader.faststream_tasks.tasks.used_run_checking', mock_checking)
            
            # 1. Submit task
            response = await service.submit(
                user_id=user_id,
                check_type=check_type,
                args=args,
                name=name,
                tag=tag
            )
            
            # Verify initial state
            assert isinstance(response, TaskResponse)
            assert response.status == TaskStatus.CREATED.value
            
            # 2. Wait for task to start running
            task_id = response.id
            running_status = await wait_for_status(service, task_id, TaskStatus.RUNNING)
            assert running_status is not None, "Task did not reach RUNNING state"
            
            # 3. Wait for task to fail
            failed_status = await wait_for_status(service, task_id, TaskStatus.FAILED)
            assert failed_status is not None, "Task did not reach FAILED state"
            assert failed_status.report is not None
            
            # 4. Parse and verify error report content
            report = CheckerReport.model_validate_json(failed_status.report)
            assert len(report.checks) == 0
            assert report.fail_reason == str(UNEXPECTED_ERROR_MESSAGE)


@pytest.mark.asyncio
async def test_task_cancellation(clean_tasks_table, clean_rabbitmq_queue, monkeypatch):
    """Test task cancellation during execution."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(broker=br)
        
        # Test data
        user_id = "test_user"
        check_type = CheckType.CLICKHOUSE
        args = {"host": "localhost", "user": "admin", "password": "admin"}
        name = "Test Task"
        tag = "test_tag"
        
        # Mock the checking function to simulate long-running task
        def mock_checking(*args, **kwargs):
            sleep(2.0)  # Simulate long work
            return CheckerReport(checks=[
                CheckReport(required=True, passed=True, check_description="Test check")
            ])
        
        with monkeypatch.context() as m:
            m.setattr('grader.faststream_tasks.tasks.used_run_checking', mock_checking)
            
            # 1. Submit task
            response = await service.submit(
                user_id=user_id,
                check_type=check_type,
                args=args,
                name=name,
                tag=tag
            )
            
            # Verify initial state
            assert isinstance(response, TaskResponse)
            assert response.status == TaskStatus.CREATED.value
            
            # 2. Wait for task to start running
            task_id = response.id
            running_status = await wait_for_status(service, task_id, TaskStatus.RUNNING)
            assert running_status is not None, "Task did not reach RUNNING state"
            
            # 3. Cancel the task
            cancellation_result = await service.cancel(task_id)
            assert cancellation_result is True, "Task cancellation failed"
            
            # 4. Wait for task to be cancelled
            cancelled_status = await wait_for_status(service, task_id, TaskStatus.CANCELLED)
            assert cancelled_status is not None, "Task did not reach CANCELLED state"


@pytest.mark.asyncio
async def test_task_listing(clean_tasks_table, clean_rabbitmq_queue, monkeypatch):
    """Test listing tasks with various filters."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(broker=br)
        
        # Mock the checking function to return quickly
        def mock_checking(*args, **kwargs):
            sleep(0.1)  # Use shorter delay for faster tests
            return CheckerReport(checks=[
                CheckReport(required=True, passed=True, check_description="Test check")
            ])
        
        with monkeypatch.context() as m:
            m.setattr('grader.faststream_tasks.tasks.used_run_checking', mock_checking)
            
            # Create tasks with different states
            task_ids = {}

            # Create a FINISHED task and wait for completion
            finished_task = await service.submit(
                user_id="test_user",
                check_type=CheckType.CLICKHOUSE,
                args={"host": "localhost"},
                name="Test Task FINISHED",
                tag="test_tag"
            )
            task_ids[TaskStatus.FINISHED] = finished_task.id
            # Wait for it to reach FINISHED state
            finished_status = await wait_for_status(service, finished_task.id, TaskStatus.FINISHED)
            assert finished_status is not None, "FINISHED task didn't reach FINISHED state in time"

            # Create a RUNNING task
            running_task = await service.submit(
                user_id="test_user",
                check_type=CheckType.CLICKHOUSE,
                args={"host": "localhost"},
                name="Test Task RUNNING",
                tag="test_tag"
            )
            task_ids[TaskStatus.RUNNING] = running_task.id
            # Wait for it to reach RUNNING state
            running_status = await wait_for_status(service, running_task.id, TaskStatus.RUNNING)
            assert running_status is not None, "RUNNING task didn't reach RUNNING state in time"
            
            # Create a CREATED task
            created_task = await service.submit(
                user_id="test_user",
                check_type=CheckType.CLICKHOUSE,
                args={"host": "localhost"},
                name="Test Task CREATED",
                tag="test_tag"
            )
            task_ids[TaskStatus.CREATED] = created_task.id
            
            # Double-check all tasks have reached their expected states before proceeding with filtering tests
            for status, task_id in task_ids.items():
                current_status = await service.status(task_id)
                assert current_status.status == status.value, f"Task {task_id} should be in {status.value} state, but is in {current_status.status}"
            
            # Test listing with different filters
            # 1. List all tasks
            all_tasks = service.list()
            assert len(all_tasks) >= len(task_ids), f"Expected at least {len(task_ids)} tasks, found {len(all_tasks)}"
            
            # 2. List by user
            user_tasks = service.list(user_id="test_user")
            assert len(user_tasks) >= len(task_ids)
            assert all(task.user_id == "test_user" for task in user_tasks)
            
            # 3. List by tag
            tagged_tasks = service.list(tag="test_tag")
            assert len(tagged_tasks) >= len(task_ids)
            assert all(task.tag == "test_tag" for task in tagged_tasks)
            
            # 4. List by status - verify each status filter returns exactly the task with that status
            for status in task_ids.keys():
                status_tasks = service.list(status=status)
                # Check that at least one task with this status exists in the results
                assert len(status_tasks) > 0, f"No tasks found with status {status.value}"
                
                # Check that our specific task with this status is in the results
                task_ids_with_status = [task.id for task in status_tasks]
                assert task_ids[status] in task_ids_with_status, f"Task with ID {task_ids[status]} not found in results for status {status.value}"
                
                # Verify all tasks in this result have the correct status
                assert all(task.status == status.value for task in status_tasks), f"Not all tasks have status {status.value}"


@pytest.mark.asyncio
async def test_task_deletion(clean_tasks_table, clean_rabbitmq_queue, monkeypatch):
    """Test task deletion."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(broker=br)
        
        # Mock the checking function to return quickly
        def mock_checking(*args, **kwargs):
            sleep(1)  # Minimal delay
            return CheckerReport(checks=[
                CheckReport(required=True, passed=True, check_description="Test check")
            ])
        
        with monkeypatch.context() as m:
            m.setattr('grader.faststream_tasks.tasks.used_run_checking', mock_checking)
            
            # Create a task
            response = await service.submit(
                user_id="test_user",
                check_type=CheckType.CLICKHOUSE,
                args={"host": "localhost"},
                name="Test Task",
                tag="test_tag"
            )
            
            # Verify task exists
            initial_status = await service.status(response.id)
            assert initial_status is not None
            
            # Delete the task
            await service.delete(response.id)
            
            # Verify task is deleted
            try:
                await service.status(response.id)
                pytest.fail("Task should not exist after deletion")
            except ValueError:
                pass  # Expected error when task doesn't exist

            await check.wait_call(timeout=5)


@pytest.mark.asyncio
async def test_delete_all_tasks(clean_tasks_table, clean_rabbitmq_queue, monkeypatch):
    """Test deleting all tasks."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(broker=br)
        
        # Mock the checking function to return quickly
        def mock_checking(*args, **kwargs):
            sleep(0.1)  # Minimal delay
            return CheckerReport(checks=[
                CheckReport(required=True, passed=True, check_description="Test check")
            ])
        
        with monkeypatch.context() as m:
            m.setattr('grader.faststream_tasks.tasks.used_run_checking', mock_checking)
            
            # Create multiple tasks
            for i in range(3):
                await service.submit(
                    user_id=f"test_user_{i}",
                    check_type=CheckType.CLICKHOUSE,
                    args={"host": "localhost"},
                    name=f"Test Task {i}",
                    tag=f"test_tag_{i}"
                )
            
            # Verify tasks exist
            initial_tasks = await service.list()
            assert len(initial_tasks) >= 3
            
            # Delete all tasks
            service.delete_all()
            
            # Verify all tasks are deleted
            remaining_tasks = await service.list()
            assert len(remaining_tasks) == 0

            logger.info(f"Waiting for 2 seconds to ensure that all tasks have been processed")
            await asyncio.sleep(2)
            
            assert check.mock.call_count == 3

