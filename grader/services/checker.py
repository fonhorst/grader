import logging
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any, Union

from faststream.rabbit import RabbitBroker
from pydantic import BaseModel

from grader.checking.checking import CheckType
from grader.db.tasks import Task, TaskStatus, create_task, get_task, delete_task, list_tasks, delete_all_tasks, update_task_status_with_isolation
from grader.faststream_tasks.schemes import CheckingTask

logger = logging.getLogger(__name__)


class TaskInfo(BaseModel):
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
    def from_db_task(cls, task: Task) -> 'TaskInfo':
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
    def __init__(self, queue: str, broker: RabbitBroker):
        self.broker = broker
        self.queue = queue

    async def submit(
        self,
        *,
        user_id: str,
        check_type: CheckType,
        args: Dict[str, Any],
        name: Optional[str] = None,
        tag: Optional[str] = None
    ) -> TaskInfo:
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
        task = await create_task(
            uid=uuid.uuid4(),
            name=name or f"Check {check_type.value}",
            user_id=user_id,
            tag=tag
        )
        
        # Create checking task for faststream
        checking_task = CheckingTask(
            task_uid=str(task.id),
            user_id=task.user_id,
            name=task.name,
            check_type=check_type,
            args=args
        )
        
        # Send task to queue
        await self.broker.publish(checking_task, self.queue)

        logger.info(f"Task {task.id} sent to queue")
        
        # Get task and convert to response
        return TaskInfo.from_db_task(task)

    async def cancel(self, task_id: Union[str, uuid.UUID]) -> bool:
        """
        Cancel a task.
        
        If the task is in CREATED status, it will be marked as CANCELLED immediately.
        If the task is in RUNNING status, it will be marked for cancellation and the checker
        will stop it at the next check point.
        
        Args:
            task_id: ID of the task to cancel
        
        Returns:
            True if the task was cancelled, False otherwise
        """
        attempt = await update_task_status_with_isolation(
                task_id=task_id,
                expected_status=TaskStatus.CREATED,
                status=TaskStatus.CANCELLED
            )
        
        if not attempt.is_success and attempt.current_status == TaskStatus.RUNNING:
            attempt = await update_task_status_with_isolation(
                task_id=task_id,
                expected_status=TaskStatus.RUNNING,
                is_cancelled=True
            )

        if not attempt.is_success:
            logger.warning(f"Cannot mark task {task_id} for cancellation: "
                            f"current status is {attempt.current_status}")
                
            return False
        
        logger.info(f"Task {task_id} was cancelled")
        return True

    async def status(self, task_id: Union[str, uuid.UUID]) -> TaskInfo:
        """
        Get status of a task.
        
        Args:
            task_id: ID of the task
            
        Returns:
            Task response with current status
        """
        task = await get_task(task_id)
        return TaskInfo.from_db_task(task)

    async def list(
        self,
        *,
        user_id: Optional[str] = None,
        tag: Optional[str] = None,
        status: Optional[TaskStatus] = None
    ) -> List[TaskInfo]:
        """
        List tasks with optional filters.
        
        Args:
            user_id: Filter by user ID
            tag: Filter by tag
            status: Filter by status
            
        Returns:
            List of matching task responses
        """
        tasks = await list_tasks(
            user_id=user_id,
            tag=tag,
            statuses=[status.value] if status else None
        )
        return [TaskInfo.from_db_task(task) for task in tasks]

    async def delete(self, task_id: Union[str, uuid.UUID]) -> None:
        """
        Delete a task.
        
        Args:
            task_id: ID of the task to delete
        """
        await delete_task(task_id)

    async def delete_all(self) -> None:
        """Delete all tasks."""
        await delete_all_tasks()
