from fastapi import APIRouter, Depends, HTTPException, status, Body
from typing import List, Optional
import uuid

from grader.services.checker import CheckerService, TaskInfo
from grader.api.schemes import (
    TaskSubmitRequest,
    TaskResponse,
    TaskListResponse,
    HealthResponse
)
from grader.checking.checking import CheckType
from grader.db.tasks import TaskStatus

router = APIRouter(prefix="/tasks", tags=["tasks"])

def get_checker_service() -> CheckerService:
    return CheckerService()

def convert_task_info_to_response(task_info: TaskInfo) -> TaskResponse:
    """Convert TaskInfo to TaskResponse."""
    return TaskResponse(
        id=task_info.id,
        user_id=task_info.user_id,
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
    user_id: str = Body(...),
    checker_service: CheckerService = Depends(get_checker_service)
) -> TaskResponse:
    """
    Submit a new checking task.
    """
    try:
        task_info = await checker_service.submit(
            user_id=user_id,
            check_type=request.check_type,
            args=request.args,
            name=request.name,
            tag=request.tag
        )
        return convert_task_info_to_response(task_info)
    except Exception as e:
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
            user_id=user_id,
            tag=tag,
            status=TaskStatus(status) if status else None
        )
        tasks = [convert_task_info_to_response(task_info) for task_info in task_infos]
        return TaskListResponse(tasks=tasks)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/{task_id}/cancel")
async def cancel_task(
    task_id: uuid.UUID,
    checker_service: CheckerService = Depends(get_checker_service)
):
    """
    Cancel a running task.
    """
    try:
        success = await checker_service.cancel(task_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Task could not be cancelled"
            )
        return {"message": "Task cancelled successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.delete("/{task_id}")
async def delete_task(
    task_id: uuid.UUID,
    checker_service: CheckerService = Depends(get_checker_service)
):
    """
    Delete a task.
    """
    try:
        await checker_service.delete(task_id)
        return {"message": "Task deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.get("/{task_id}/report", response_model=dict)
async def get_task_report(
    task_id: uuid.UUID,
    checker_service: CheckerService = Depends(get_checker_service)
) -> dict:
    """
    Get the report from a finished task.
    """
    try:
        task_info = await checker_service.status(task_id)
        if not task_info.report:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Report not found or task not completed"
            )
        # Assuming report is a JSON string
        return {"report": task_info.report}
    except Exception as e:
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