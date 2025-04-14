from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime
import uuid

from grader.checking.checking import CheckType


class TaskSubmitRequest(BaseModel):
    check_type: CheckType
    args: dict
    student_id: str
    name: Optional[str] = None
    tag: Optional[str] = None


class TaskResponse(BaseModel):
    id: uuid.UUID
    student_id: str
    student_name: str
    group: str
    course_id: str
    course_name: str
    name: str
    tag: Optional[str] = None
    attachment: Optional[str] = None
    status: str
    status_updated_at: datetime
    submit_time: datetime
    end_time: Optional[datetime] = None
    report: Optional[str] = None


class TaskReportResponse(BaseModel):
    report: str


class MessageResponse(BaseModel):
    message: str


class TaskListResponse(BaseModel):
    tasks: List[TaskResponse]


class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str = "1.0.0"


# Course-related DTOs
class CourseCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    tag: Optional[str] = None


class CourseUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    tag: Optional[str] = None


class CourseResponse(BaseModel):
    id: uuid.UUID
    name: str
    description: Optional[str] = None
    tag: Optional[str] = None


class CourseListResponse(BaseModel):
    courses: List[CourseResponse]


# Student-related DTOs
class StudentCreateRequest(BaseModel):
    name: str
    course_id: uuid.UUID
    group: Optional[str] = None
    tag: Optional[str] = None


class StudentUpdateRequest(BaseModel):
    name: Optional[str] = None
    group: Optional[str] = None
    tag: Optional[str] = None
    course_id: Optional[uuid.UUID] = None


class StudentResponse(BaseModel):
    id: uuid.UUID
    name: str
    group: Optional[str] = None
    tag: Optional[str] = None
    course_id: uuid.UUID
    course_name: str


class StudentListResponse(BaseModel):
    students: List[StudentResponse]
