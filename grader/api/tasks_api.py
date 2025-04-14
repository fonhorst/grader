from fastapi import APIRouter, Depends, HTTPException, status, Body
from typing import Optional
import uuid
import logging

from grader.services.checker import CheckerService, TaskInfo
from grader.schemes import (
    MessageResponse,
    TaskReportResponse,
    TaskSubmitRequest,
    TaskResponse,
    TaskListResponse,
    HealthResponse
)
from grader.db.tasks import TaskStatus

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["tasks"])


async def get_checker_service() -> CheckerService:
    from grader.faststream_tasks.tasks import broker, broker_queue_name
    await broker.connect()
    return CheckerService(queue=broker_queue_name, broker=broker)


def convert_task_info_to_response(task_info: TaskInfo) -> TaskResponse:
    """Convert TaskInfo to TaskResponse."""
    return TaskResponse(
        id=task_info.id,
        student_id=str(task_info.student_id),
        student_name=task_info.student_name,
        group=task_info.group,
        course_id=str(task_info.course_id),
        course_name=task_info.course_name,
        name=task_info.name,
        tag=task_info.tag,
        attachment=task_info.attachment,
        status=task_info.status,
        status_updated_at=task_info.status_updated_at,
        submit_time=task_info.submit_time,
        end_time=task_info.end_time,
        report=task_info.report
    )


@router.post("/", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def submit_task(
    request: TaskSubmitRequest = Body(...),
    checker_service: CheckerService = Depends(get_checker_service)
) -> TaskResponse:
    """
    Submit a new checking task.
    """
    try:
        task_info = await checker_service.submit(
            student_id=request.student_id,
            check_type=request.check_type,
            args=request.args,
            name=request.name,
            tag=request.tag
        )
        return convert_task_info_to_response(task_info)
    except Exception as e:
        logger.error(
            f"Failed to submit task. User ID: {request.student_id}, Check Type: {request.check_type}, "
            f"Name: {request.name}, Tag: {request.tag}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: uuid.UUID,
    checker_service: CheckerService = Depends(get_checker_service)
) -> TaskResponse:
    """
    Get information about a specific task.
    """
    try:
        task_info = await checker_service.status(task_id)
        return convert_task_info_to_response(task_info)
    except Exception as e:
        logger.error(
            f"Failed to get task. Task ID: {task_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task not found: {str(e)}"
        )


@router.get("/", response_model=TaskListResponse)
async def list_tasks(
    user_id: Optional[str] = None,
    tag: Optional[str] = None,
    status: Optional[str] = None,
    checker_service: CheckerService = Depends(get_checker_service)
) -> TaskListResponse:
    """
    List all tasks with optional filtering.
    """
    try:
        task_infos = await checker_service.list(
            student_id=user_id,
            tag=tag,
            status=TaskStatus(status) if status else None
        )
        tasks = [convert_task_info_to_response(task_info) for task_info in task_infos]
        return TaskListResponse(tasks=tasks)
    except Exception as e:
        logger.error(
            f"Failed to list tasks. Filters: User ID: {user_id}, Tag: {tag}, Status: {status}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
    

@router.post("/{task_id}/cancel", response_model=MessageResponse)
async def cancel_task(
    task_id: uuid.UUID,
    checker_service: CheckerService = Depends(get_checker_service)
) -> MessageResponse:
    """
    Cancel a running task.
    """
    try:
        success = await checker_service.cancel(task_id)
        if not success:
            logger.error(
                f"Task could not be cancelled. Task ID: {task_id}",
                exc_info=True
            )
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Task could not be cancelled"
            )
        return MessageResponse(message="Task cancelled successfully")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to cancel task. Task ID: {task_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/{task_id}", response_model=MessageResponse)
async def delete_task(
    task_id: uuid.UUID,
    checker_service: CheckerService = Depends(get_checker_service)
) -> MessageResponse:
    """
    Delete a task.
    """
    try:
        await checker_service.delete(task_id)
        return MessageResponse(message="Task deleted successfully")
    except Exception as e:
        logger.error(
            f"Failed to delete task. Task ID: {task_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{task_id}/report", response_model=TaskReportResponse)
async def get_task_report(
    task_id: uuid.UUID,
    checker_service: CheckerService = Depends(get_checker_service)
) -> TaskReportResponse:
    """
    Get the report from a finished task.
    """
    try:
        task_info = await checker_service.status(task_id)
        if not task_info.report:
            logger.error(
                f"Report not found or task not completed. Task ID: {task_id}, Status: {task_info.status}"
            )
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Report not found or task not completed"
            )
        return TaskReportResponse(report=task_info.report)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Failed to get task report. Task ID: {task_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Check the health of the service.
    """
    return HealthResponse()

