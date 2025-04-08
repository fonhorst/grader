from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime
import uuid

from grader.checking.checking import CheckType

class TaskSubmitRequest(BaseModel):
    check_type: CheckType
    args: dict
    name: Optional[str] = None
    tag: Optional[str] = None

class TaskResponse(BaseModel):
    id: uuid.UUID
    user_id: str
    name: str
    tag: Optional[str] = None
    attachment: Optional[str] = None
    status: str
    status_updated_at: datetime
    submit_time: datetime
    end_time: Optional[datetime] = None
    report: Optional[str] = None

class TaskListResponse(BaseModel):
    tasks: List[TaskResponse]

class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str = "1.0.0"
