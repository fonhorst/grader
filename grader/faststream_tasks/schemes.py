from typing import Any, Dict
from pydantic import BaseModel

from grader.checking.base import CheckerReport
from grader.checking.checking import CheckType


class FastStreamCheckTaskException(Exception):
    pass


class CheckingTask(BaseModel):
    task_uid: str
    user_id: str
    name: str
    check_type: CheckType
    args: Dict[str, Any]

    @property
    def full_name(self) -> str:
        return f" Task {self.task_uid}: {self.name} of check type {self.check_type} (user {self.user_id})"


class CheckingResult(BaseModel):
    task_uid: str
    report: CheckerReport

