from typing import Any, Dict
from pydantic import BaseModel

from grader.checking.base import CheckerReport
from grader.checking.checking import CheckType


class CheckingTask(BaseModel):
    task_uid: str
    user_id: str
    check_type: CheckType
    args: Dict[str, Any]


class CheckingResult(BaseModel):
    task_uid: str
    report: CheckerReport

