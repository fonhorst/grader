import logging
from typing import Optional, List, Dict, Any, Union
import uuid
from datetime import datetime

from faststream.rabbit import RabbitBroker
from pydantic import BaseModel

from grader.checking.checking import CheckType
from grader.db.tasks import (
    Task, TaskStatus, create_task, get_task, update_task_status,
    delete_task, list_tasks, delete_all_tasks
)
from grader.faststream_tasks.schemes import CheckingTask
from grader.faststream_tasks.tasks import broker

logger = logging.getLogger(__name__)


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

    @classmethod
    def from_db_task(cls, task: Task) -> 'TaskResponse':
        return cls(
            id=task.id,
            user_id=task.user_id,
            name=task.name,
            tag=task.tag,
            attachment=task.attachment,
            status=task.status,
            status_updated_at=task.status_updated_at,
            submit_time=task.submit_time,
            end_time=task.end_time,
            report=task.report
        )


class CheckerService:
    def __init__(self, broker: Optional[RabbitBroker] = None):
        self.broker = broker or broker

    async def submit(
        self,
        *,
        user_id: str,
        check_type: CheckType,
        args: Dict[str, Any],
        name: Optional[str] = None,
        tag: Optional[str] = None
    ) -> TaskResponse:
        """
        Submit a new checking task.
        
        Args:
            user_id: ID of the user submitting the task
            check_type: Type of check to perform
            args: Arguments for the checker
            name: Optional name for the task
            tag: Optional tag for the task
            
        Returns:
            Created task response
        """
        # Create task in database
        task = create_task(
            uid=uuid.uuid4(),
            name=name or f"Check {check_type.value}",
            user_id=user_id,
            tag=tag
        )
        
        # Create checking task for faststream
        checking_task = CheckingTask(
            task_uid=str(task.id),
            user_id=task.user_id,
            check_type=check_type,
            args=args
        )
        
        # Send task to queue
        await self.broker.publish(checking_task, "test-queue")
        
        # Get task and convert to response
        return TaskResponse.from_db_task(task)

    async def cancel(self, task_id: Union[str, uuid.UUID]) -> None:
        """
        Cancel a running task.
        
        Args:
            task_id: ID of the task to cancel
        """
        task = get_task(task_id)
        if task.status == TaskStatus.RUNNING:
            update_task_status(task_id, TaskStatus.CANCELLED)
        else:
            raise ValueError(f"Cannot cancel task in status {task.status}")

    def status(self, task_id: Union[str, uuid.UUID]) -> TaskResponse:
        """
        Get status of a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Task response with current status
        """
        task = get_task(task_id)
        return TaskResponse.from_db_task(task)

    def list(
        self,
        *,
        user_id: Optional[str] = None,
        tag: Optional[str] = None,
        status: Optional[TaskStatus] = None
    ) -> List[TaskResponse]:
        """
        List tasks with optional filters.
        
        Args:
            user_id: Filter by user ID
            tag: Filter by tag
            status: Filter by status
            
        Returns:
            List of matching task responses
        """
        tasks = list_tasks(
            user_id=user_id,
            tag=tag,
            statuses=[status.value] if status else None
        )
        return [TaskResponse.from_db_task(task) for task in tasks]

    def delete(self, task_id: Union[str, uuid.UUID]) -> None:
        """
        Delete a task.
        
        Args:
            task_id: ID of the task to delete
        """
        delete_task(task_id)

    def delete_all(self) -> None:
        """Delete all tasks."""
        delete_all_tasks()
