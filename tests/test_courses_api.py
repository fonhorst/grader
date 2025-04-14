import uuid
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from grader.services.checker import CourseInfo, StudentCourseService


class MockStudentCourseService:
    """Mock implementation of StudentCourseService for testing."""
    
    def __init__(self, mock_course_info: CourseInfo):
        self.mock_course_info = mock_course_info
        self.mock_course_list = [mock_course_info]
    
    async def create_course(self, *, name: str, description: str = None, tag: str = None) -> CourseInfo:
        return self.mock_course_info
    
    async def get_course(self, course_id: uuid.UUID) -> CourseInfo:
        if str(course_id) == str(self.mock_course_info.id):
            return self.mock_course_info
        raise ValueError("Course not found")
    
    async def list_courses(self, *, name: str = None, tag: str = None) -> list[CourseInfo]:
        return self.mock_course_list
    
    async def update_course(self, course_id: uuid.UUID, *, name: str = None, description: str = None, tag: str = None) -> CourseInfo:
        if str(course_id) == str(self.mock_course_info.id):
            return self.mock_course_info
        raise ValueError("Course not found")
    
    async def delete_course(self, course_id: uuid.UUID) -> None:
        if str(course_id) != str(self.mock_course_info.id):
            raise ValueError("Course not found")


class MockStudentCourseServiceWithFailures(MockStudentCourseService):
    """Mock implementation that simulates failures."""
    
    async def delete_course(self, course_id: uuid.UUID) -> None:
        raise ValueError("Cannot delete course with associated students")


def create_test_client(mock_service: MockStudentCourseService):
    """Create test client with dependency override."""
    from grader.api.courses_api import router as courses_router, get_student_course_service
    app = FastAPI()
    app.include_router(courses_router)
    app.dependency_overrides[get_student_course_service] = lambda: mock_service
    return TestClient(app)


@pytest.fixture
def mock_course_info():
    return CourseInfo(
        id=uuid.uuid4(),
        name="Test Course",
        description="Test Description",
        tag="test_tag"
    )


@pytest.mark.asyncio
async def test_create_course(mock_course_info):
    """Test creating a new course."""
    mock_service = MockStudentCourseService(mock_course_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.post(
        "/courses/",
        json={
            "name": "Test Course",
            "description": "Test Description",
            "tag": "test_tag"
        }
    )
    
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == mock_course_info.name
    assert data["description"] == mock_course_info.description
    assert data["tag"] == mock_course_info.tag


@pytest.mark.asyncio
async def test_get_course(mock_course_info):
    """Test getting course information."""
    mock_service = MockStudentCourseService(mock_course_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.get(f"/courses/{mock_course_info.id}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(mock_course_info.id)
    assert data["name"] == mock_course_info.name
    assert data["description"] == mock_course_info.description
    assert data["tag"] == mock_course_info.tag


@pytest.mark.asyncio
async def test_list_courses(mock_course_info):
    """Test listing courses with filters."""
    mock_service = MockStudentCourseService(mock_course_info)
    test_client = create_test_client(mock_service)
    
    # Test without filters
    response = test_client.get("/courses/")
    assert response.status_code == 200
    data = response.json()
    assert len(data["courses"]) == 1
    assert data["courses"][0]["id"] == str(mock_course_info.id)
    
    # Test with filters
    response = test_client.get(f"/courses/?name={mock_course_info.name}&tag={mock_course_info.tag}")
    assert response.status_code == 200
    data = response.json()
    assert len(data["courses"]) == 1
    assert data["courses"][0]["name"] == mock_course_info.name
    assert data["courses"][0]["tag"] == mock_course_info.tag


@pytest.mark.asyncio
async def test_update_course(mock_course_info):
    """Test updating a course."""
    mock_service = MockStudentCourseService(mock_course_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.put(
        f"/courses/{mock_course_info.id}",
        json={
            "name": "Updated Course",
            "description": "Updated Description",
            "tag": "updated_tag"
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == str(mock_course_info.id)
    assert data["name"] == mock_course_info.name
    assert data["description"] == mock_course_info.description
    assert data["tag"] == mock_course_info.tag


@pytest.mark.asyncio
async def test_delete_course(mock_course_info):
    """Test deleting a course."""
    mock_service = MockStudentCourseService(mock_course_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.delete(f"/courses/{mock_course_info.id}")
    
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "Course deleted successfully"


# Error cases
@pytest.mark.asyncio
async def test_get_nonexistent_course():
    """Test getting a course that doesn't exist."""
    mock_service = MockStudentCourseService(None)
    test_client = create_test_client(mock_service)
    
    response = test_client.get(f"/courses/{uuid.uuid4()}")
    assert response.status_code == 404
    data = response.json()
    assert "Course not found" in data["detail"]


@pytest.mark.asyncio
async def test_delete_course_with_students(mock_course_info):
    """Test deleting a course that has associated students."""
    mock_service = MockStudentCourseServiceWithFailures(mock_course_info)
    test_client = create_test_client(mock_service)
    
    response = test_client.delete(f"/courses/{mock_course_info.id}")
    assert response.status_code == 400
    data = response.json()
    assert "Cannot delete course with associated students" in data["detail"] 