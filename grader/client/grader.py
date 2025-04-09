from typing import Optional
import uuid
import asyncio
import time
import aiohttp

from grader.schemes import TaskListResponse, TaskReportResponse, TaskResponse, TaskSubmitRequest
from grader.db.tasks import TaskStatus


class GraderApiException(Exception):
    """Exception raised for errors in the API calls."""
    def __init__(self, message: str, status_code: Optional[int] = None, detail: Optional[str] = None):
        self.message = message
        self.status_code = status_code
        self.detail = detail
        super().__init__(self.message)


class GraderApiTimeoutException(GraderApiException):
    """Exception raised when waiting for task completion times out."""
    pass


class GraderAPIClient:
    """Client for interacting with the Grader API."""
    
    def __init__(self, base_url: str = "http://localhost:8080"):
        """Initialize the client with base URL."""
        self.base_url = base_url.rstrip('/')
        self._session = None
    
    async def __aenter__(self) -> 'GraderAPIClient':
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
            self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        """Get the current session or create a new one."""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> dict:
        """Make HTTP request to the API and handle errors."""
        url = f"{self.base_url}{endpoint}"
        session = self._get_session()
        
        try:
            async with session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            status_code = getattr(e, 'status', None)
            detail = None
            if isinstance(e, aiohttp.ClientResponseError):
                try:
                    error_data = await response.json()
                    detail = error_data.get('detail')
                except:
                    detail = await response.text()
            
            raise GraderApiException(
                message=str(e),
                status_code=status_code,
                detail=detail
            )

    async def submit_task(self, request: TaskSubmitRequest, wait_timeout: Optional[float] = None, poll_interval: float = 1.0) -> TaskResponse:
        """Submit a new task and optionally wait for completion.
        
        Args:
            request: The task submission request
            wait_timeout: If not None, wait for task completion for this many seconds.
                        If 0, wait indefinitely. If task doesn't complete within timeout,
                        raises GraderApiTimeoutException.
            poll_interval: The interval to poll for task completion in seconds.It has no effect if wait_timeout is None.
        """
        # The API now expects user_id as part of the request
        data = await self._make_request('POST', '/tasks/', json=request.model_dump())
        response = TaskResponse(**data)
        
        if wait_timeout is not None:
            start_time = time.time()
            while True:
                task = await self.get_task(response.id)
                if task.status in [TaskStatus.FINISHED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    return task
                
                if wait_timeout > 0 and (time.time() - start_time) > wait_timeout:
                    raise GraderApiTimeoutException(
                        message=f"Task {response.id} did not complete within {wait_timeout} seconds",
                        detail=f"Current status: {task.status}"
                    )
                
                await asyncio.sleep(poll_interval)  # Poll every second
        
        return response

    async def get_task(self, task_id: uuid.UUID) -> TaskResponse:
        """Get information about a specific task."""
        data = await self._make_request('GET', f'/tasks/{task_id}')
        return TaskResponse(**data)

    async def list_tasks(self, user_id: Optional[str] = None, tag: Optional[str] = None, status: Optional[str] = None) -> TaskListResponse:
        """List tasks with optional filtering."""
        params = {k: v for k, v in {'user_id': user_id, 'tag': tag, 'status': status}.items() if v is not None}
        data = await self._make_request('GET', '/tasks/', params=params)
        return TaskListResponse(**data)

    async def delete_task(self, task_id: uuid.UUID) -> None:
        """Delete a task."""
        await self._make_request('DELETE', f'/tasks/{task_id}')

    async def get_task_report(self, task_id: uuid.UUID) -> TaskReportResponse:
        """Get the report from a finished task."""
        data = await self._make_request('GET', f'/tasks/{task_id}/report')
        return TaskReportResponse(**data)

    async def cancel_task(self, task_id: uuid.UUID) -> TaskResponse:
        """Cancel a running task."""
        data = await self._make_request('POST', f'/tasks/{task_id}/cancel')
        return TaskResponse(**data)

