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
async def test_task_submit_positive():
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
async def test_task_submit_negative():
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