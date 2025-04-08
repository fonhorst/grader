import uuid
from datetime import datetime
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from faststream.rabbit import TestRabbitBroker

from grader.api.tasks_api import router as tasks_router, get_checker_service
from grader.checking.base import CheckReport
from grader.checking.checking import CheckType, CheckerReport
from grader.db.tasks import TaskStatus
from grader.services.checker import TaskInfo, CheckerService


class MockCheckerService:
    """Mock implementation of CheckerService for testing."""
    
    def __init__(self, mock_task_info: TaskInfo):
        self.mock_task_info = mock_task_info
        self.mock_task_list = [mock_task_info]
    
    async def submit(self, *args, **kwargs) -> TaskInfo:
        return self.mock_task_info
    
    async def status(self, task_id: uuid.UUID) -> TaskInfo:
        if str(task_id) == str(self.mock_task_info.id):
            return self.mock_task_info
        raise ValueError("Task not found")
    
    async def list(self, *args, **kwargs) -> list[TaskInfo]:
        return self.mock_task_list
    
    async def cancel(self, task_id: uuid.UUID) -> bool:
        return True
    
    async def delete(self, task_id: uuid.UUID) -> None:
        pass


class MockCheckerServiceWithFailures(MockCheckerService):
    """Mock implementation that simulates failures."""
    
    async def cancel(self, task_id: uuid.UUID) -> bool:
        return False
    
    async def status(self, task_id: uuid.UUID) -> TaskInfo:
        raise ValueError("Task not found")


class MockCheckerServiceNoReport(MockCheckerService):
    """Mock implementation for a task without a report."""
    
    def __init__(self):
        self.mock_task_info = TaskInfo(
            id=uuid.uuid4(),
            user_id="test_user",
            name="Test Task",
            tag="test_tag",
            status=TaskStatus.RUNNING.value,
            status_updated_at=datetime.now(),
            submit_time=datetime.now(),
            report=None
        )


# Create test app
app = FastAPI()
app.include_router(tasks_router)


@pytest.fixture
def test_client():
    return TestClient(app)


@pytest.fixture
def mock_task_info():
    return TaskInfo(
        id=uuid.uuid4(),
        user_id="test_user",
        name="Test Task",
        tag="test_tag",
        status=TaskStatus.FINISHED.value,
        status_updated_at=datetime.now(),
        submit_time=datetime.now(),
        end_time=datetime.now(),
        report=CheckerReport(checks=[
            CheckReport(required=True, passed=True, check_description="Test check")
        ]).model_dump_json()
    )


@pytest.mark.asyncio
async def test_submit_task(test_client, monkeypatch, mock_task_info):
    """Test submitting a new task."""
    mock_service = MockCheckerService(mock_task_info)
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        response = test_client.post(
            "/tasks/",
            json={
                "check_type": CheckType.CLICKHOUSE.value,
                "args": {"host": "localhost"},
                "name": "Test Task",
                "tag": "test_tag"
            },
            headers={"user-id": "test_user"}
        )
        
        assert response.status_code == 201
        data = response.json()
        assert data["user_id"] == "test_user"
        assert data["name"] == "Test Task"
        assert data["tag"] == "test_tag"


@pytest.mark.asyncio
async def test_get_task(test_client, monkeypatch, mock_task_info):
    """Test getting task information."""
    mock_service = MockCheckerService(mock_task_info)
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        response = test_client.get(f"/tasks/{mock_task_info.id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == str(mock_task_info.id)
        assert data["user_id"] == mock_task_info.user_id
        assert data["name"] == mock_task_info.name


@pytest.mark.asyncio
async def test_list_tasks(test_client, monkeypatch, mock_task_info):
    """Test listing tasks with filters."""
    mock_service = MockCheckerService(mock_task_info)
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        # Test without filters
        response = test_client.get("/tasks/")
        assert response.status_code == 200
        data = response.json()
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["id"] == str(mock_task_info.id)
        
        # Test with filters
        response = test_client.get("/tasks/?user_id=test_user&tag=test_tag&status=FINISHED")
        assert response.status_code == 200
        data = response.json()
        assert len(data["tasks"]) == 1
        assert data["tasks"][0]["user_id"] == "test_user"
        assert data["tasks"][0]["tag"] == "test_tag"
        assert data["tasks"][0]["status"] == TaskStatus.FINISHED.value


@pytest.mark.asyncio
async def test_cancel_task(test_client, monkeypatch, mock_task_info):
    """Test cancelling a task."""
    mock_service = MockCheckerService(mock_task_info)
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        response = test_client.post(f"/tasks/{mock_task_info.id}/cancel")
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Task cancelled successfully"


@pytest.mark.asyncio
async def test_delete_task(test_client, monkeypatch, mock_task_info):
    """Test deleting a task."""
    mock_service = MockCheckerService(mock_task_info)
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        response = test_client.delete(f"/tasks/{mock_task_info.id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Task deleted successfully"


@pytest.mark.asyncio
async def test_get_task_report(test_client, monkeypatch, mock_task_info):
    """Test getting task report."""
    mock_service = MockCheckerService(mock_task_info)
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        response = test_client.get(f"/tasks/{mock_task_info.id}/report")
        
        assert response.status_code == 200
        data = response.json()
        assert "report" in data
        report_data = data["report"]
        assert isinstance(report_data, str)


# Error cases
@pytest.mark.asyncio
async def test_get_nonexistent_task(test_client, monkeypatch):
    """Test getting a task that doesn't exist."""
    mock_service = MockCheckerServiceWithFailures(None)
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        response = test_client.get(f"/tasks/{uuid.uuid4()}")
        assert response.status_code == 404
        data = response.json()
        assert "Task not found" in data["detail"]


@pytest.mark.asyncio
async def test_cancel_failed_task(test_client, monkeypatch):
    """Test cancelling a task that can't be cancelled."""
    mock_service = MockCheckerServiceWithFailures(None)
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        response = test_client.post(f"/tasks/{uuid.uuid4()}/cancel")
        assert response.status_code == 400
        data = response.json()
        assert "Task could not be cancelled" in data["detail"]


@pytest.mark.asyncio
async def test_get_report_no_report(test_client, monkeypatch):
    """Test getting report for a task that doesn't have one."""
    mock_service = MockCheckerServiceNoReport()
    
    with monkeypatch.context() as m:
        m.setattr("grader.api.tasks_api.get_checker_service", lambda: mock_service)
        
        response = test_client.get(f"/tasks/{mock_service.mock_task_info.id}/report")
        assert response.status_code == 404
        data = response.json()
        assert "Report not found or task not completed" in data["detail"]

 