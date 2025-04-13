from time import sleep
import pytest
import asyncio
from faststream.rabbit import TestRabbitBroker
import uuid
from typing import Tuple

import pytest_asyncio

from grader.checking.base import CheckReport
from grader.checking.checking import CheckType, CheckerReport
from grader.db.tasks import TaskStatus
from grader.faststream_tasks.schemes import FastStreamCheckTaskException
from grader.faststream_tasks.tasks import UNEXPECTED_ERROR_MESSAGE, broker, check
from grader.services.checker import CheckerService, TaskInfo
from tests.conftest import wait_for_status
import logging

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture(scope="session")
async def test_course_and_student(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name):
    """Create a test course and student for task-related tests."""
    async with TestRabbitBroker(broker, with_real=False) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # Create test course
        course = await service.create_course(
            name="Test Course",
            description="Course for testing",
            tag="test"
        )
        
        # Create test student
        students = await service.create_students([{
            "name": "Test Student",
            "course_id": course.id,
            "group": "A",
            "tag": "test_student"
        }])
        student = students[0]
        
        yield course, student
        
        # Cleanup
        await service.delete_student(student.id)
        await service.delete_course(course.id)


@pytest.mark.asyncio
async def test_task_submit_positive(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name, monkeypatch, test_course_and_student):
    """Test complete task lifecycle with successful execution."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    course, student = test_course_and_student
    
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # Test data
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
                student_id=str(student.id),
                check_type=check_type,
                args=args,
                name=name,
                tag=tag
            )
            
            # Verify initial state
            assert isinstance(response, TaskInfo)
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
async def test_task_submit_negative(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name, monkeypatch, test_course_and_student):
    """Test complete task lifecycle with failed execution."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    course, student = test_course_and_student
    
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # Test data
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
                student_id=str(student.id),
                check_type=check_type,
                args=args,
                name=name,
                tag=tag
            )
            
            # Verify initial state
            assert isinstance(response, TaskInfo)
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
async def test_task_cancellation(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name, monkeypatch, test_course_and_student):
    """Test task cancellation during execution."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    course, student = test_course_and_student
    
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # Test data
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
                student_id=str(student.id),
                check_type=check_type,
                args=args,
                name=name,
                tag=tag
            )
            
            # Verify initial state
            assert isinstance(response, TaskInfo)
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
async def test_task_listing(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name, monkeypatch, test_course_and_student):
    """Test listing tasks with various filters."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    course, student = test_course_and_student
    
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # Mock the checking function to return quickly
        def mock_checking(*args, **kwargs):
            sleep(0.5)  # Use shorter delay for faster tests
            return CheckerReport(checks=[
                CheckReport(required=True, passed=True, check_description="Test check")
            ])
        
        with monkeypatch.context() as m:
            m.setattr('grader.faststream_tasks.tasks.used_run_checking', mock_checking)
            
            # Create tasks with different states
            task_ids = {}

            # Create a FINISHED task and wait for completion
            finished_task = await service.submit(
                student_id=str(student.id),
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
                student_id=str(student.id),
                check_type=CheckType.CLICKHOUSE,
                args={"host": "localhost"},
                name="Test Task RUNNING",
                tag="test_tag"
            )
            task_ids[TaskStatus.RUNNING] = running_task.id
            # Wait for it to reach RUNNING state
            running_status = await wait_for_status(service, running_task.id, TaskStatus.RUNNING)
            assert running_status is not None, "RUNNING task didn't reach RUNNING state in time"
            
            # Double-check all tasks have reached their expected states before proceeding with filtering tests
            for status, task_id in task_ids.items():
                current_status = await service.status(task_id)
                assert current_status.status == status.value, f"Task {task_id} should be in {status.value} state, but is in {current_status.status}"
            
            # Test listing with different filters
            # 1. List all tasks
            all_tasks = await service.list()
            assert len(all_tasks) >= len(task_ids), f"Expected at least {len(task_ids)} tasks, found {len(all_tasks)}"
            
            # 2. List by user
            user_tasks = await service.list(student_id=str(student.id))
            assert len(user_tasks) >= len(task_ids)
            assert all(task.student_id == str(student.id) for task in user_tasks)
            
            # 3. List by tag
            tagged_tasks = await service.list(tag="test_tag")
            assert len(tagged_tasks) >= len(task_ids)
            assert all(task.tag == "test_tag" for task in tagged_tasks)
            
            # 4. List by status - verify each status filter returns exactly the task with that status
            for status in task_ids.keys():
                status_tasks = await service.list(status=status)
                # Check that at least one task with this status exists in the results
                assert len(status_tasks) > 0, f"No tasks found with status {status.value}"
                
                # Check that our specific task with this status is in the results
                task_ids_with_status = [task.id for task in status_tasks]
                assert task_ids[status] in task_ids_with_status, f"Task with ID {task_ids[status]} not found in results for status {status.value}"
                
                # Verify all tasks in this result have the correct status
                assert all(task.status == status.value for task in status_tasks), f"Not all tasks have status {status.value}"


@pytest.mark.asyncio
async def test_task_deletion(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name, monkeypatch, test_course_and_student):
    """Test task deletion."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    course, student = test_course_and_student
    
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
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
                student_id=str(student.id),
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
async def test_delete_all_tasks(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name, monkeypatch, test_course_and_student):
    """Test deleting all tasks."""
    logger.info("Ensure clean tasks table. Number of tasks: %d", clean_tasks_table)
    logger.info("Ensure clean rabbitmq queue. Queue name: %s", clean_rabbitmq_queue)
    course, student = test_course_and_student
    
    async with TestRabbitBroker(broker, with_real=True) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
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
                    student_id=str(student.id),
                    check_type=CheckType.CLICKHOUSE,
                    args={"host": "localhost"},
                    name=f"Test Task {i}",
                    tag=f"test_tag_{i}"
                )
            
            # Verify tasks exist
            initial_tasks = await service.list()
            assert len(initial_tasks) >= 3
            
            # Delete all tasks
            await service.delete_all()
            
            # Verify all tasks are deleted
            remaining_tasks = await service.list()
            assert len(remaining_tasks) == 0

            logger.info(f"Waiting for 2 seconds to ensure that all tasks have been processed")
            await asyncio.sleep(2)
            
            assert check.mock.call_count == 3


@pytest.mark.asyncio
async def test_course_operations(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name):
    """Test course CRUD operations."""
    async with TestRabbitBroker(broker, with_real=False) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # 1. Create course
        course = await service.create_course(
            name="Python 101",
            description="Introduction to Python",
            tag="python"
        )
        assert course.name == "Python 101"
        assert course.description == "Introduction to Python"
        assert course.tag == "python"
        
        # 2. Get course
        retrieved_course = await service.get_course(course.id)
        assert retrieved_course.id == course.id
        assert retrieved_course.name == course.name
        
        # 3. Update course
        updated_course = await service.update_course(
            course_id=course.id,
            name="Advanced Python",
            description="Advanced Python Programming",
            tag="advanced"
        )
        assert updated_course.id == course.id
        assert updated_course.name == "Advanced Python"
        assert updated_course.description == "Advanced Python Programming"
        assert updated_course.tag == "advanced"
        
        # 4. List courses
        courses = await service.list_courses()
        assert len(courses) >= 1
        assert any(c.id == course.id for c in courses)
        
        # Test filtering
        filtered_courses = await service.list_courses(tag="advanced")
        assert len(filtered_courses) >= 1
        assert all(c.tag == "advanced" for c in filtered_courses)
        
        # 5. Delete course
        await service.delete_course(course.id)
        
        # Verify course is deleted
        with pytest.raises(ValueError):
            await service.get_course(course.id)


@pytest.mark.asyncio
async def test_student_operations(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name):
    """Test student CRUD operations."""
    async with TestRabbitBroker(broker, with_real=False) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # 1. Create course first
        course = await service.create_course(
            name="Test Course",
            description="Test Course Description",
            tag="test"
        )
        
        # 2. Create single student
        student = await service.create_students([{
            "name": "John Doe",
            "course_id": course.id,
            "group": "A",
            "tag": "test_student"
        }])[0]
        
        assert student.name == "John Doe"
        assert student.group == "A"
        assert student.tag == "test_student"
        assert student.course_id == course.id
        assert student.course_name == course.name
        
        # 3. Get student
        retrieved_student = await service.get_student(student.id)
        assert retrieved_student.id == student.id
        assert retrieved_student.name == student.name
        
        # 4. Update student
        updated_student = await service.update_student(
            student_id=student.id,
            name="Jane Doe",
            group="B",
            tag="updated_student"
        )
        assert updated_student.id == student.id
        assert updated_student.name == "Jane Doe"
        assert updated_student.group == "B"
        assert updated_student.tag == "updated_student"
        
        # 5. List students
        students = await service.list_students()
        assert len(students) >= 1
        assert any(s.id == student.id for s in students)
        
        # Test filtering
        filtered_students = await service.list_students(group="B")
        assert len(filtered_students) >= 1
        assert all(s.group == "B" for s in filtered_students)
        
        # Test course filtering
        course_students = await service.list_students(course_id=course.id)
        assert len(course_students) >= 1
        assert all(s.course_id == course.id for s in course_students)
        
        # 6. Delete student
        await service.delete_student(student.id)
        
        # Verify student is deleted
        with pytest.raises(ValueError):
            await service.get_student(student.id)


@pytest.mark.asyncio
async def test_bulk_student_creation(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name):
    """Test creating multiple students in a single transaction."""
    async with TestRabbitBroker(broker, with_real=False) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # Create course
        course = await service.create_course(
            name="Bulk Test Course",
            description="Course for bulk student creation",
            tag="bulk_test"
        )
        
        # Create multiple students
        students_data = [
            {
                "name": f"Student {i}",
                "course_id": course.id,
                "group": "A" if i % 2 == 0 else "B",
                "tag": "bulk_test"
            }
            for i in range(5)
        ]
        
        students = await service.create_students(students_data)
        
        # Verify all students were created
        assert len(students) == 5
        for i, student in enumerate(students):
            assert student.name == f"Student {i}"
            assert student.course_id == course.id
            assert student.group == ("A" if i % 2 == 0 else "B")
            assert student.tag == "bulk_test"
            assert student.course_name == course.name
        
        # Verify students can be retrieved
        for student in students:
            retrieved = await service.get_student(student.id)
            assert retrieved.id == student.id
            assert retrieved.name == student.name


@pytest.mark.asyncio
async def test_student_course_relationship(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name):
    """Test student-course relationship operations."""
    async with TestRabbitBroker(broker, with_real=False) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # Create two courses
        course1 = await service.create_course(name="Course 1", tag="test")
        course2 = await service.create_course(name="Course 2", tag="test")
        
        # Create student in first course
        student = await service.create_students([{
            "name": "Test Student",
            "course_id": course1.id,
            "group": "A"
        }])[0]
        
        assert student.course_id == course1.id
        assert student.course_name == course1.name
        
        # Move student to second course
        updated_student = await service.update_student(
            student_id=student.id,
            course_id=course2.id
        )
        
        assert updated_student.course_id == course2.id
        assert updated_student.course_name == course2.name
        
        # Verify student appears in new course's list
        course2_students = await service.list_students(course_id=course2.id)
        assert any(s.id == student.id for s in course2_students)
        
        # Verify student no longer appears in old course's list
        course1_students = await service.list_students(course_id=course1.id)
        assert not any(s.id == student.id for s in course1_students)


@pytest.mark.asyncio
async def test_error_handling(clean_tasks_table, clean_rabbitmq_queue, broker_queue_name):
    """Test error handling for invalid operations."""
    async with TestRabbitBroker(broker, with_real=False) as br:
        service = CheckerService(queue=broker_queue_name, broker=br)
        
        # Test getting non-existent course
        with pytest.raises(ValueError):
            await service.get_course(uuid.uuid4())
        
        # Test getting non-existent student
        with pytest.raises(ValueError):
            await service.get_student(uuid.uuid4())
        
        # Test updating non-existent course
        with pytest.raises(ValueError):
            await service.update_course(
                course_id=uuid.uuid4(),
                name="Non-existent"
            )
        
        # Test updating non-existent student
        with pytest.raises(ValueError):
            await service.update_student(
                student_id=uuid.uuid4(),
                name="Non-existent"
            )
        
        # Test deleting non-existent course
        await service.delete_course(uuid.uuid4())  # Should not raise
        
        # Test deleting non-existent student
        await service.delete_student(uuid.uuid4())  # Should not raise
        
        # Test creating student with non-existent course
        with pytest.raises(ValueError):
            await service.create_students([{
                "name": "Test Student",
                "course_id": uuid.uuid4(),
                "group": "A"
            }])

