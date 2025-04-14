import uuid
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from grader.services.checker import StudentInfo, StudentCourseService


class MockStudentCourseService:
    """Mock implementation of StudentCourseService for testing."""
    
    def __init__(self, mock_student_info: StudentInfo):
        self.mock_student_info = mock_student_info
        self.mock_student_list = [mock_student_info] if mock_student_info else []
    
    async def create_students(self, students_data: list[dict]) -> list[StudentInfo]:
        return [self.mock_student_info]
    
    async def get_student(self, student_id: uuid.UUID) -> StudentInfo:
        if not self.mock_student_info or str(student_id) != str(self.mock_student_info.id):
            raise ValueError("Student not found")
        return self.mock_student_info
    
    async def list_students(self, *, name: str = None, group: str = None, tag: str = None, course_id: str = None) -> list[StudentInfo]:
        return self.mock_student_list
    
    async def update_student(self, student_id: uuid.UUID, *, name: str = None, group: str = None, tag: str = None, course_id: uuid.UUID = None) -> StudentInfo:
        if not self.mock_student_info or str(student_id) != str(self.mock_student_info.id):
            raise ValueError("Student not found")
        return self.mock_student_info
    
    async def delete_student(self, student_id: uuid.UUID) -> None:
        if not self.mock_student_info or str(student_id) != str(self.mock_student_info.id):
            raise ValueError("Student not found")


class MockStudentCourseServiceWithFailures(MockStudentCourseService):
    """Mock implementation that simulates failures."""
    
    async def delete_student(self, student_id: uuid.UUID) -> None:
        raise ValueError("Cannot delete student with associated tasks")


def create_test_client(mock_service: MockStudentCourseService):
    """Create test client with dependency override."""
    from grader.api.students_api import router as students_router, get_student_course_service
    app = FastAPI()
    app.include_router(students_router)
    app.dependency_overrides[get_student_course_service] = lambda: mock_service
    return TestClient(app)


@pytest.fixture
def mock_student_info():
    return StudentInfo(
        id=uuid.uuid4(),
        name="Test Student",
        group="Test Group",
        tag="test_tag",
        course_id=uuid.uuid4(),
        course_name="Test Course"
    )


@pytest.mark.asyncio
async def test_create_students(mock_student_info):
    """Test creating new students."""
    mock_service = MockStudentCourseService(mock_student_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.post(
        "/students/",
        json=[{
            "name": "Test Student",
            "course_id": str(mock_student_info.course_id),
            "group": "Test Group",
            "tag": "test_tag"
        }]
    )
    
    assert response.status_code == 201
    data = response.json()
    assert len(data) == 1
    assert data[0]["name"] == mock_student_info.name
    assert data[0]["group"] == mock_student_info.group
    assert data[0]["tag"] == mock_student_info.tag
    assert data[0]["course_id"] == str(mock_student_info.course_id)
    assert data[0]["course_name"] == mock_student_info.course_name


@pytest.mark.asyncio
async def test_get_student(mock_student_info):
    """Test getting student information."""
    mock_service = MockStudentCourseService(mock_student_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.get(f"/students/{mock_student_info.id}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(mock_student_info.id)
    assert data["name"] == mock_student_info.name
    assert data["group"] == mock_student_info.group
    assert data["tag"] == mock_student_info.tag
    assert data["course_id"] == str(mock_student_info.course_id)
    assert data["course_name"] == mock_student_info.course_name


@pytest.mark.asyncio
async def test_list_students(mock_student_info):
    """Test listing students with filters."""
    mock_service = MockStudentCourseService(mock_student_info)
    test_client = create_test_client(mock_service)
    
    # Test without filters
    response = test_client.get("/students/")
    assert response.status_code == 200
    data = response.json()
    assert len(data["students"]) == 1
    assert data["students"][0]["id"] == str(mock_student_info.id)
    
    # Test with filters
    response = test_client.get(
        f"/students/?name={mock_student_info.name}&group={mock_student_info.group}"
        f"&tag={mock_student_info.tag}&course_id={mock_student_info.course_id}"
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["students"]) == 1
    assert data["students"][0]["name"] == mock_student_info.name
    assert data["students"][0]["group"] == mock_student_info.group
    assert data["students"][0]["tag"] == mock_student_info.tag
    assert data["students"][0]["course_id"] == str(mock_student_info.course_id)


@pytest.mark.asyncio
async def test_update_student(mock_student_info):
    """Test updating a student."""
    mock_service = MockStudentCourseService(mock_student_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.put(
        f"/students/{mock_student_info.id}",
        json={
            "name": "Updated Student",
            "group": "Updated Group",
            "tag": "updated_tag",
            "course_id": str(uuid.uuid4())
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(mock_student_info.id)
    assert data["name"] == mock_student_info.name
    assert data["group"] == mock_student_info.group
    assert data["tag"] == mock_student_info.tag
    assert data["course_id"] == str(mock_student_info.course_id)
    assert data["course_name"] == mock_student_info.course_name


@pytest.mark.asyncio
async def test_delete_student(mock_student_info):
    """Test deleting a student."""
    mock_service = MockStudentCourseService(mock_student_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.delete(f"/students/{mock_student_info.id}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Student deleted successfully"


# Error cases
@pytest.mark.asyncio
async def test_get_nonexistent_student():
    """Test getting a student that doesn't exist."""
    mock_service = MockStudentCourseService(None)
    test_client = create_test_client(mock_service)
    
    response = test_client.get(f"/students/{uuid.uuid4()}")
    assert response.status_code == 404
    data = response.json()
    assert "Student not found" in data["detail"]


@pytest.mark.asyncio
async def test_delete_student_with_tasks(mock_student_info):
    """Test deleting a student that has associated tasks."""
    mock_service = MockStudentCourseServiceWithFailures(mock_student_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.delete(f"/students/{mock_student_info.id}")
    assert response.status_code == 400
    data = response.json()
    assert "Cannot delete student with associated tasks" in data["detail"] 