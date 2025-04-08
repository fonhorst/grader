from typing import Optional, List
import uuid
import requests
from pydantic import BaseModel
from datetime import datetime

from grader.schemes import TaskListResponse, TaskReportResponse, TaskResponse, TaskSubmitRequest


class GraderApiException(Exception):
    """Exception raised for errors in the API calls."""
    def __init__(self, message: str, status_code: Optional[int] = None, detail: Optional[str] = None):
        self.message = message
        self.status_code = status_code
        self.detail = detail
        super().__init__(self.message)


class GraderAPIClient:
    """Client for interacting with the Grader API."""
    
    def __init__(self, base_url: str = "http://localhost:8080"):
        """Initialize the client with base URL."""
        self.base_url = base_url.rstrip('/')
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make HTTP request to the API and handle errors."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            detail = None
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    detail = error_data.get('detail')
                except:
                    detail = e.response.text
            
            raise GraderApiException(
                message=str(e),
                status_code=status_code,
                detail=detail
            )

    def submit_task(self, request: TaskSubmitRequest) -> TaskResponse:
        """Submit a new task."""
        data = self._make_request('POST', '/tasks/', json=request.model_dump())
        return TaskResponse(**data)

    def get_task(self, task_id: uuid.UUID) -> TaskResponse:
        """Get information about a specific task."""
        data = self._make_request('GET', f'/tasks/{task_id}')
        return TaskResponse(**data)

    def list_tasks(self, user_id: Optional[str] = None, tag: Optional[str] = None, status: Optional[str] = None) -> TaskListResponse:
        """List tasks with optional filtering."""
        params = {k: v for k, v in {'user_id': user_id, 'tag': tag, 'status': status}.items() if v is not None}
        data = self._make_request('GET', '/tasks/', params=params)
        return TaskListResponse(**data)

    def delete_task(self, task_id: uuid.UUID) -> None:
        """Delete a task."""
        self._make_request('DELETE', f'/tasks/{task_id}')

    def get_task_report(self, task_id: uuid.UUID) -> TaskReportResponse:
        """Get the report from a finished task."""
        data = self._make_request('GET', f'/tasks/{task_id}/report')
        return TaskReportResponse(**data)

    def cancel_task(self, task_id: uuid.UUID) -> TaskResponse:
        """Cancel a running task."""
        data = self._make_request('POST', f'/tasks/{task_id}/cancel')
        return TaskResponse(**data)

