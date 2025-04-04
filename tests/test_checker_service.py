import pytest
import uuid
from datetime import datetime
from unittest.mock import patch, AsyncMock
import asyncio

from faststream.rabbit import TestRabbitBroker
from sqlalchemy.exc import OperationalError

from grader.checking.checking import CheckType, CheckerReport, Check
from grader.db.tasks import TaskStatus, UpdateStatusAttempt
from grader.faststream_tasks.schemes import CheckingTask, CheckingResult
from grader.faststream_tasks.tasks import broker
from grader.services.checker import CheckerService, TaskResponse
from tests.conftest import wait_for_status


@pytest.mark.asyncio
async def test_submit_task():
    """Test submitting a new task."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        # Test data
        user_id = "test_user"
        check_type = CheckType.CLICKHOUSE
        args = {"host": "localhost", "user": "admin", "password": "admin"}
        name = "Test Task"
        tag = "test_tag"
        
        # Submit task
        response = await service.submit(
            user_id=user_id,
            check_type=check_type,
            args=args,
            name=name,
            tag=tag
        )
        
        # Verify response
        assert isinstance(response, TaskResponse)
        assert response.user_id == user_id
        assert response.name == name
        assert response.tag == tag
        assert response.status == TaskStatus.CREATED.value
        
        # Verify task was published to queue
        published_task = await br.publisher.get_message()
        assert published_task is not None
        assert published_task.task_uid == str(response.id)
        assert published_task.user_id == user_id
        assert published_task.name == name
        assert published_task.check_type == check_type
        assert published_task.args == args


@pytest.mark.asyncio
async def test_cancel_task_created():
    """Test cancelling a task in CREATED state."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        # Create a task
        task_id = uuid.uuid4()
        with patch('grader.db.tasks.update_task_status_with_isolation') as mock_update:
            mock_update.return_value = UpdateStatusAttempt(is_success=True)
            
            # Cancel task
            result = await service.cancel(task_id)
            
            # Verify result
            assert result is True
            mock_update.assert_called_once_with(
                task_id=task_id,
                expected_status=TaskStatus.CREATED,
                status=TaskStatus.CANCELLED
            )


@pytest.mark.asyncio
async def test_cancel_task_running():
    """Test cancelling a task in RUNNING state."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        # Create a task
        task_id = uuid.uuid4()
        with patch('grader.db.tasks.update_task_status_with_isolation') as mock_update:
            # First attempt fails because task is RUNNING
            mock_update.side_effect = [
                UpdateStatusAttempt(is_success=False, current_status=TaskStatus.RUNNING),
                UpdateStatusAttempt(is_success=True)
            ]
            
            # Cancel task
            result = await service.cancel(task_id)
            
            # Verify result
            assert result is True
            assert mock_update.call_count == 2
            mock_update.assert_called_with(
                task_id=task_id,
                expected_status=TaskStatus.RUNNING,
                is_cancelled=True
            )


@pytest.mark.asyncio
async def test_cancel_task_failed():
    """Test cancelling a task that cannot be cancelled."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        # Create a task
        task_id = uuid.uuid4()
        with patch('grader.db.tasks.update_task_status_with_isolation') as mock_update:
            # Both attempts fail
            mock_update.side_effect = [
                UpdateStatusAttempt(is_success=False, current_status=TaskStatus.FINISHED),
                UpdateStatusAttempt(is_success=False, current_status=TaskStatus.FINISHED)
            ]
            
            # Cancel task
            result = await service.cancel(task_id)
            
            # Verify result
            assert result is False
            assert mock_update.call_count == 2


@pytest.mark.asyncio
async def test_get_task_status():
    """Test getting task status."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        # Create a task
        task_id = uuid.uuid4()
        with patch('grader.db.tasks.get_task') as mock_get:
            # Mock task data
            mock_task = type('Task', (), {
                'id': task_id,
                'user_id': 'test_user',
                'name': 'Test Task',
                'tag': 'test_tag',
                'status': TaskStatus.RUNNING.value,
                'status_updated_at': datetime.now(),
                'submit_time': datetime.now(),
                'end_time': None,
                'report': None
            })
            mock_get.return_value = mock_task
            
            # Get status
            response = service.status(task_id)
            
            # Verify response
            assert isinstance(response, TaskResponse)
            assert response.id == task_id
            assert response.status == TaskStatus.RUNNING.value


@pytest.mark.asyncio
async def test_list_tasks():
    """Test listing tasks with filters."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        with patch('grader.db.tasks.list_tasks') as mock_list:
            # Mock task data
            mock_task = type('Task', (), {
                'id': uuid.uuid4(),
                'user_id': 'test_user',
                'name': 'Test Task',
                'tag': 'test_tag',
                'status': TaskStatus.RUNNING.value,
                'status_updated_at': datetime.now(),
                'submit_time': datetime.now(),
                'end_time': None,
                'report': None
            })
            mock_list.return_value = [mock_task]
            
            # List tasks
            response = service.list(
                user_id='test_user',
                tag='test_tag',
                status=TaskStatus.RUNNING
            )
            
            # Verify response
            assert len(response) == 1
            assert isinstance(response[0], TaskResponse)
            assert response[0].user_id == 'test_user'
            assert response[0].tag == 'test_tag'
            assert response[0].status == TaskStatus.RUNNING.value
            
            # Verify list_tasks was called with correct filters
            mock_list.assert_called_once_with(
                user_id='test_user',
                tag='test_tag',
                statuses=[TaskStatus.RUNNING.value]
            )


@pytest.mark.asyncio
async def test_delete_task():
    """Test deleting a task."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        task_id = uuid.uuid4()
        with patch('grader.db.tasks.delete_task') as mock_delete:
            # Delete task
            service.delete(task_id)
            
            # Verify delete was called
            mock_delete.assert_called_once_with(task_id)


@pytest.mark.asyncio
async def test_delete_all_tasks():
    """Test deleting all tasks."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        with patch('grader.db.tasks.delete_all_tasks') as mock_delete_all:
            # Delete all tasks
            service.delete_all()
            
            # Verify delete_all was called
            mock_delete_all.assert_called_once()


@pytest.mark.asyncio
async def test_task_lifecycle_positive():
    """Test complete task lifecycle with successful execution."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        # Test data
        user_id = "test_user"
        check_type = CheckType.CLICKHOUSE
        args = {"host": "localhost", "user": "admin", "password": "admin"}
        name = "Test Task"
        tag = "test_tag"
        
        # Mock the checking function to return a successful report after delay
        mock_report = CheckerReport(checks=[
            Check(name="test_check", passed=True, message="Test passed")
        ])
        
        async def mock_checking(*args, **kwargs):
            await asyncio.sleep(0.5)  # Simulate work
            return mock_report
        
        with patch('grader.checking.checking.run_checking', side_effect=mock_checking):
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
            assert report.checks[0].name == "test_check"
            assert report.checks[0].passed is True
            assert report.checks[0].message == "Test passed"


@pytest.mark.asyncio
async def test_task_lifecycle_negative():
    """Test complete task lifecycle with failed execution."""
    async with TestRabbitBroker(broker) as br:
        service = CheckerService(broker=br)
        
        # Test data
        user_id = "test_user"
        check_type = CheckType.CLICKHOUSE
        args = {"host": "localhost", "user": "admin", "password": "admin"}
        name = "Test Task"
        tag = "test_tag"
        
        # Mock the checking function to raise an exception after delay
        test_error = ValueError("Test error in checking function")
        
        async def mock_checking(*args, **kwargs):
            await asyncio.sleep(0.5)  # Simulate work
            raise test_error
        
        with patch('grader.checking.checking.run_checking', side_effect=mock_checking):
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
            assert report.fail_reason == str(test_error) 