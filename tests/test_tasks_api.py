import uuid
from datetime import datetime
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from faststream.rabbit import TestRabbitBroker

from grader.api.tasks_api import router as tasks_router
from grader.checking.base import CheckReport
from grader.checking.checking import CheckType, CheckerReport
from grader.db.tasks import TaskStatus
from grader.services.checker import TaskInfo, CheckerService

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
    
    async def mock_submit(*args, **kwargs):
        return mock_task_info
    
    with monkeypatch.context() as m:
        # Mock the submit method of CheckerService
        m.setattr(CheckerService, "submit", mock_submit)
        
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
    
    async def mock_status(*args, **kwargs):
        return mock_task_info
    
    with monkeypatch.context() as m:
        m.setattr(CheckerService, "status", mock_status)
        
        response = test_client.get(f"/tasks/{mock_task_info.id}")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == str(mock_task_info.id)
        assert data["user_id"] == mock_task_info.user_id
        assert data["name"] == mock_task_info.name


@pytest.mark.asyncio
async def test_list_tasks(test_client, monkeypatch, mock_task_info):
    """Test listing tasks with filters."""
    
    async def mock_list(*args, **kwargs):
        return [mock_task_info]
    
    with monkeypatch.context() as m:
        m.setattr(CheckerService, "list", mock_list)
        
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
    
    async def mock_cancel(*args, **kwargs):
        return True
    
    with monkeypatch.context() as m:
        m.setattr(CheckerService, "cancel", mock_cancel)
        
        response = test_client.post(f"/tasks/{mock_task_info.id}/cancel")
        
        assert response.status_code == 200
        data = response.json()
        # assert data["message"] == "Task cancelled successfully"


@pytest.mark.asyncio
async def test_delete_task(test_client, monkeypatch, mock_task_info):
    """Test deleting a task."""
    
    async def mock_delete(*args, **kwargs):
        return None
    
    with monkeypatch.context() as m:
        m.setattr(CheckerService, "delete", mock_delete)
        
        response = test_client.delete(f"/tasks/{mock_task_info.id}")
        
        assert response.status_code == 200
        data = response.json()
        # assert data["message"] == "Task deleted successfully"


@pytest.mark.asyncio
async def test_get_task_report(test_client, monkeypatch, mock_task_info):
    """Test getting task report."""
    
    async def mock_status(*args, **kwargs):
        return mock_task_info
    
    with monkeypatch.context() as m:
        m.setattr(CheckerService, "status", mock_status)
        
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
    
    async def mock_status(*args, **kwargs):
        raise ValueError("Task not found")
    
    with monkeypatch.context() as m:
        m.setattr(CheckerService, "status", mock_status)
        
        response = test_client.get(f"/tasks/{uuid.uuid4()}")
        assert response.status_code == 404
        data = response.json()
        assert "Task not found" in data["detail"]


@pytest.mark.asyncio
async def test_cancel_failed_task(test_client, monkeypatch):
    """Test cancelling a task that can't be cancelled."""
    
    async def mock_cancel(*args, **kwargs):
        return False
    
    with monkeypatch.context() as m:
        m.setattr(CheckerService, "cancel", mock_cancel)
        
        response = test_client.post(f"/tasks/{uuid.uuid4()}/cancel")
        assert response.status_code == 400
        data = response.json()
        assert "Task could not be cancelled" in data["detail"]


@pytest.mark.asyncio
async def test_get_report_no_report(test_client, monkeypatch):
    """Test getting report for a task that doesn't have one."""
    
    task_info = TaskInfo(
        id=uuid.uuid4(),
        user_id="test_user",
        name="Test Task",
        tag="test_tag",
        status=TaskStatus.RUNNING.value,
        status_updated_at=datetime.now(),
        submit_time=datetime.now(),
        report=None
    )
    
    async def mock_status(*args, **kwargs):
        return task_info
    
    with monkeypatch.context() as m:
        m.setattr(CheckerService, "status", mock_status)
        
        response = test_client.get(f"/tasks/{task_info.id}/report")
        assert response.status_code == 404
        data = response.json()
        assert "Report not found or task not completed" in data["detail"] 