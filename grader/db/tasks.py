import datetime
import enum
import logging
import os
import uuid
from typing import Optional, Dict, Union, Any, List, cast, Tuple

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import String, UUID, TIMESTAMP, ForeignKey, inspect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, joinedload

from grader.env import ENV_VAR_DB_CONN, ENV_VAR_ECHO_DB_QUERY

logger = logging.getLogger(__name__)

DateTimeType = Optional[Union[str, float, datetime.datetime]]

# Convert connection string to async version
DB_CONN = os.environ.get(ENV_VAR_DB_CONN, 'postgresql://postgres:postgres@localhost:5432/grader')
ASYNC_DB_CONN = DB_CONN.replace('postgresql://', 'postgresql+asyncpg://')

logger.warning("DB_CONN %s" % ASYNC_DB_CONN)

# Create async engine and session maker
engine = create_async_engine(ASYNC_DB_CONN, echo=os.environ.get(ENV_VAR_ECHO_DB_QUERY, "no") == "yes")
# https://docs.sqlalchemy.org/en/20/orm/sessionF_transaction.html#setting-isolation-for-individual-sessions
isolated_engine = engine.execution_options(isolation_level="REPEATABLE READ")
AsyncSessionBuilder = async_sessionmaker(engine)

class ImpossibleTaskStatusTransition(Exception):
    pass


class TaskStatus(str, enum.Enum):
    CREATED = 'created'
    RUNNING = 'running'
    FINISHED = 'finished'
    FAILED = 'failed'
    CANCELLED = 'cancelled'

    @classmethod
    def is_terminal(cls, status: 'TaskStatus') -> bool:
        return status in [TaskStatus.FAILED, TaskStatus.FINISHED, TaskStatus.CANCELLED]

    @classmethod
    def can_proceed(cls,
                    status_from: 'TaskStatus',
                    status_to: 'TaskStatus',
                    tolerable_equal_from_and_to: bool = False) -> bool:
        if status_from == status_to:
            return False

        if status_from == cls.CREATED:
            return True

        if status_from == cls.RUNNING and status_to != cls.CREATED:
            return True

        return False


class Base(DeclarativeBase):
    type_annotation_map = {
        uuid.UUID: UUID(as_uuid=True),
        datetime.datetime: TIMESTAMP(timezone=True),
        Dict[str, Any]: JSONB(),
        Dict[str, Union[str, int, float, bool]]: JSONB()
    }


# Task statuses:
# 0 - Created
# 1 - Running
# 2 - Finished
# 3 - Failed
# 4 - Cancelled
class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)

    user_id: Mapped[str] = mapped_column(String(50), nullable=False)

    name: Mapped[str] = mapped_column(String(50), nullable=False)

    tag: Mapped[str] = mapped_column(String(50), nullable=True)

    attachment: Mapped[str] = mapped_column(String(50), nullable=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False)

    status_updated_at: Mapped[datetime.datetime] = mapped_column(nullable=False)

    submit_time: Mapped[datetime.datetime] = mapped_column(nullable=False)

    end_time: Mapped[datetime.datetime] = mapped_column(nullable=True)

    report: Mapped[str] = mapped_column(nullable=True)

    is_cancelled: Mapped[bool] = mapped_column(default=False)

    def __repr__(self) -> str:
        return f"Task(id={self.id!r}, user_id={self.user_id!r}, name={self.name!r}, status={self.status!r})"


async def create_tables() -> bool:
    """
    Create the tasks table if it doesn't exist.
    Returns True if the table was created, False if it already existed.
    """
    logger.info("Creating required tables")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def _standartize_datetime(dt: DateTimeType) -> datetime.datetime:
    if isinstance(dt, str):
        dt = datetime.datetime.fromisoformat(dt)
    elif isinstance(dt, float):
        dt = datetime.datetime.fromtimestamp(dt)
    return dt


def handle_name_like(filters: list, param: Mapped[str], value: Optional[Union[str, List[str]]]):
    if not value:
        return

    if isinstance(value, list):
        expr = param.in_(value)
    elif '%' in value:
        expr = param.like(value)
    else:
        expr = (param == value)

    filters.append(expr)


async def create_task(
    *,
    uid: Optional[uuid.UUID] = None,
    name: str,
    user_id: str,
    tag: Optional[str] = None,
    attachment: Optional[str] = None,
    submit_time: Optional[datetime.datetime] = None
) -> Task:
    async with AsyncSessionBuilder() as session:
        dt = datetime.datetime.now()
        task_id = uid or uuid.uuid4()
        task = Task(
            id=task_id,
            name=name,
            user_id=user_id,
            tag=tag,
            attachment=attachment,
            status=TaskStatus.CREATED.value,
            status_updated_at=submit_time or dt,
            submit_time=submit_time or dt
        )
        session.add(task)
        await session.commit()
        
        # Refresh the task in the current session to ensure we have a fully loaded object
        task = await session.get(Task, task_id)
        if not task:
            raise ValueError(f"Failed to create task with ID {task_id}")
            
        # Clone task attributes to avoid DetachedInstanceError
        await session.expunge(task)  # Detach from session without cascading to DB
        
        return task


async def get_task(task_id: Union[str, uuid.UUID]) -> Task:
    async with AsyncSessionBuilder() as session:
        task = await session.get(Task, task_id)
        if not task:
            raise ValueError(f"Task with id {task_id} not found")
        return task


async def update_task_status(task_id: Union[str, uuid.UUID], status: TaskStatus):
    async with AsyncSessionBuilder(bind=isolated_engine) as session:
        async with session.begin():
            attempt = 0
            max_retries = 3
            while attempt < max_retries:
                try:
                    stmt = await session.execute(
                        session.query(Task).filter(Task.id == task_id).with_for_update()
                    )
                    task = stmt.scalar_one()
                    curr_status = TaskStatus(task.status)

                    if not TaskStatus.can_proceed(curr_status, status):
                        raise ImpossibleTaskStatusTransition(
                            f"Cannot change status from {curr_status} to {status}"
                        )

                    dt = datetime.datetime.now()
                    task.status = status.value
                    task.status_updated_at = dt

                    if TaskStatus.is_terminal(status):
                        task.end_time = dt

                    break
                except OperationalError:
                    logger.error(
                        "Unsuccessful attempt to update task status "
                        "(task_uid=%s) due to operational exception. "
                        "Retry %s of %s",
                        task_id, attempt + 1, max_retries,
                        exc_info=True
                    )
                    attempt += 1
                    if attempt >= max_retries:
                        raise


async def delete_task(task_id: Union[str, uuid.UUID]):
    async with AsyncSessionBuilder() as session:
        task = await session.get(Task, task_id)
        if task:
            await session.delete(task)
            await session.commit()


async def list_tasks(
        uids: Optional[List[str]] = None,
        name: Optional[Union[str, List[str]]] = None,
        user_id: Optional[Union[str, List[str]]] = None,
        tag: Optional[Union[str, List[str]]] = None,
        statuses: Optional[List[str]] = None,
        submit_time: Optional[Tuple[DateTimeType, DateTimeType]] = None) -> List[Task]:
    async with AsyncSessionBuilder() as session:
        query = session.query(Task)

        filters = []
        if uids:
            filters.append(Task.id.in_([uuid.UUID(uid) for uid in uids]))

        vparams = [
            (name, Task.name),
            (user_id, Task.user_id),
            (tag, Task.tag),
            (statuses, Task.status)
        ]
        for value, param in vparams:
            handle_name_like(filters, param, value)

        if submit_time:
            start, end = submit_time
            if start:
                filters.append(Task.submit_time >= _standartize_datetime(start))
            if end:
                filters.append(Task.submit_time <= _standartize_datetime(end))

        if filters:
            query = query.filter(*filters)

        result = await session.execute(query)
        return list(result.scalars().all())


async def delete_all_tasks():
    async with AsyncSessionBuilder() as session:
        await session.execute(session.query(Task).delete())
        await session.commit()


class UpdateStatusAttempt:
    def __init__(self, is_success: bool, current_status: Optional[TaskStatus] = None):
        self.is_success = is_success
        self.current_status = current_status


async def update_task_status_with_isolation(*,
    task_id: Union[str, uuid.UUID],
    expected_status: Optional[Union[TaskStatus, List[TaskStatus]]] = None,
    status: Optional[TaskStatus] = None,
    report: Optional[str] = None,
    is_cancelled: Optional[bool] = None,
    max_attempts: int = 3
) -> UpdateStatusAttempt:
    """
    Update task status with isolation level, checking expected status.
    
    Args:
        task_id: ID of task to update
        expected_status: Expected current status, can be list of statuses or None to skip check
        status: New status to set, if None the status will not be changed
        report: Report to set, if None the report will not be changed
        is_cancelled: Whether to mark task as cancelled
        max_attempts: Maximum number of attempts for updating
        
    Returns:
        UpdateStatusAttempt with success flag and current status
    """
    attempt = 0
    
    # Convert single status to list
    if expected_status is not None and not isinstance(expected_status, list):
        expected_status = [expected_status]
    
    while attempt < max_attempts:
        try:
            async with AsyncSessionBuilder(bind=isolated_engine) as session:
                async with session.begin():
                    stmt = await session.execute(
                        session.query(Task).filter(Task.id == task_id).with_for_update()
                    )
                    
                    task = stmt.scalar_one()
                    curr_status = TaskStatus(task.status)
                    
                    # Check current status
                    if expected_status is not None and curr_status not in expected_status:
                        return UpdateStatusAttempt(is_success=False, current_status=curr_status)
                    
                    dt = datetime.datetime.now()
                    
                    # Update values if provided
                    if status is not None:
                        if not TaskStatus.can_proceed(curr_status, status):
                            return UpdateStatusAttempt(is_success=False, current_status=curr_status)
                        
                        task.status = status.value
                        task.status_updated_at = dt
                        
                        if TaskStatus.is_terminal(status):
                            task.end_time = dt
                    
                    if report is not None:
                        task.report = report
                        
                    if is_cancelled is not None:
                        task.is_cancelled = is_cancelled
                    
                    await session.commit()
                    return UpdateStatusAttempt(is_success=True, current_status=curr_status)
        
        except OperationalError:
            logger.error(
                "Unsuccessful attempt to update task status "
                "(task_uid=%s) due to operational exception. "
                "Retry %s of %s",
                task_id, attempt + 1, max_attempts,
                exc_info=True
            )
            attempt += 1
            if attempt >= max_attempts:
                raise

