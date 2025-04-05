import datetime
import enum
import logging
import os
import uuid
from typing import Optional, Dict, Union, Any, List, cast, Tuple

from sqlalchemy import create_engine, String, UUID, TIMESTAMP, ForeignKey, inspect
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker, relationship, joinedload

from grader.env import ENV_VAR_DB_CONN, ENV_VAR_ECHO_DB_QUERY

logger = logging.getLogger(__name__)

DateTimeType = Optional[Union[str, float, datetime.datetime]]

DB_CONN = os.environ.get(ENV_VAR_DB_CONN, 'postgresql://postgres:postgres@localhost:5432/grader')

logger.warning("DB_CONN %s" % DB_CONN)

# TODO: move to async version
engine = create_engine(DB_CONN, echo=os.environ.get(ENV_VAR_ECHO_DB_QUERY, "yes") == "yes")
# https://docs.sqlalchemy.org/en/20/orm/sessionF_transaction.html#setting-isolation-for-individual-sessions
isolated_engine = engine.execution_options(isolation_level="REPEATABLE READ")
SessionBuilder = sessionmaker(engine)


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


def create_tasks_table() -> bool:
    """
    Create the tasks table if it doesn't exist.
    Returns True if the table was created, False if it already existed.
    """
    logger.info("Checking if tasks table exists")
    inspector = inspect(engine)
    if not inspector.has_table("tasks"):
        logger.info("Creating tasks table")
        Base.metadata.create_all(engine)
        return True
    else:
        logger.info("Tasks table already exists")
        return False


# TODO: revise the implementation of all functions below this line, refactor the code to synchronize them with the change in the classes above in this file
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


def create_task(
    *,
    uid: Optional[uuid.UUID] = None,
    name: str,
    user_id: str,
    tag: Optional[str] = None,
    attachment: Optional[str] = None,
    submit_time: Optional[datetime.datetime] = None
) -> Task:
    with SessionBuilder() as session:
        dt = datetime.datetime.now()
        task = Task(
            id=uid or uuid.uuid4(),
            name=name,
            user_id=user_id,
            tag=tag,
            attachment=attachment,
            status=TaskStatus.CREATED.value,
            status_updated_at=submit_time or dt,
            submit_time=submit_time or dt
        )
        session.add(task)
        session.commit()

    return get_task(task.id)


def get_task(task_id: Union[str, uuid.UUID]) -> Task:
    with SessionBuilder() as session:
        task = cast(Task, session.get(Task, task_id))
        if not task:
            raise ValueError(f"Task with id {task_id} not found")
        return task


def update_task_status(task_id: Union[str, uuid.UUID], status: TaskStatus):
    with SessionBuilder(bind=isolated_engine) as session:
        with session.begin():
            attempt = 0
            max_retries = 3
            while attempt < max_retries:
                try:
                    task = cast(Task, session.query(Task).filter(Task.id == task_id).with_for_update().one())
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


def delete_task(task_id: Union[str, uuid.UUID]):
    with SessionBuilder() as session:
        task = session.get(Task, task_id)
        if task:
            session.delete(task)
            session.commit()


def list_tasks(
        uids: Optional[List[str]] = None,
        name: Optional[Union[str, List[str]]] = None,
        user_id: Optional[Union[str, List[str]]] = None,
        tag: Optional[Union[str, List[str]]] = None,
        statuses: Optional[List[str]] = None,
        submit_time: Optional[Tuple[DateTimeType, DateTimeType]] = None) -> List[Task]:
    with SessionBuilder() as session:
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

        return cast(List[Task], query.all())


def delete_all_tasks():
    with SessionBuilder() as session:
        session.query(Task).delete()
        session.commit()


class UpdateStatusAttempt:
    def __init__(self, is_success: bool, current_status: Optional[TaskStatus] = None):
        self.is_success = is_success
        self.current_status = current_status


def update_task_status_with_isolation(*,
    task_id: Union[str, uuid.UUID],
    expected_status: Optional[Union[TaskStatus, List[TaskStatus]]] = None,
    status: Optional[TaskStatus] = None,
    report: Optional[str] = None,
    is_cancelled: Optional[bool] = None,
    max_attempts: int = 3
) -> UpdateStatusAttempt:
    """
    Update task status in an isolated transaction.
    
    Args:
        task_id: ID of the task to update
        status: New status to set
        expected_status: Optional status or list of statuses that the task should be in before update
        report: Optional report to update
        is_cancelled: Optional flag to set task as cancelled
        max_attempts: Maximum number of retry attempts for operational errors
        
    Returns:
        UpdateStatusAttempt with success status and current status if failed
    """
    attempt = 0
    while attempt < max_attempts:
        with SessionBuilder(bind=isolated_engine) as session:
            try:
                with session.begin():
                    task = cast(Task, session.query(Task).filter(Task.id == task_id).with_for_update().one())
                    
                    # Check if task is in any of the expected statuses
                    if expected_status is not None:
                        expected_statuses = [expected_status] if isinstance(expected_status, TaskStatus) else expected_status
                        if task.status not in [status.value for status in expected_statuses]:
                            return UpdateStatusAttempt(
                                is_success=False,
                                current_status=TaskStatus(task.status)
                            )      

                    if status:
                        curr_status = TaskStatus(task.status)
                        if not TaskStatus.can_proceed(curr_status, status):
                            return UpdateStatusAttempt(
                                is_success=False,
                                current_status=curr_status
                            )

                        dt = datetime.datetime.now()
                        task.status = status.value
                        task.status_updated_at = dt

                        if TaskStatus.is_terminal(status):
                            task.end_time = dt

                    if report is not None:
                        task.report = report
                    
                    if is_cancelled is not None:
                        task.is_cancelled = is_cancelled

                    # Store the result before committing
                    return UpdateStatusAttempt(is_success=True)
                    
            except OperationalError as e:
                attempt += 1
                if attempt >= max_attempts:
                    logger.warning(f"All {max_attempts} attempts to update task {task_id} failed with operational error: {str(e)}")
                    break
                else:
                    logger.debug(f"Attempt {attempt} of {max_attempts} failed for task {task_id}: {str(e)}")
                    continue
            
    # Try to get current status in a new transaction
    try:
        with SessionBuilder() as status_session:
            task = status_session.query(Task).filter(Task.id == task_id).first()
            if not task:
                raise ValueError(f"Task {task_id} not found")
            return UpdateStatusAttempt(is_success=False, current_status=TaskStatus(task.status))
    except Exception as status_error:
        logger.error(f"Failed to get current status for task {task_id} after all attempts failed: {str(status_error)}", exc_info=True)
        raise  # Re-raise the original error if we can't get the current status

def mark_task_cancelled(task_id: Union[str, uuid.UUID], expected_status: TaskStatus) -> UpdateStatusAttempt:
    """
    Update task status in an isolated transaction.
    
    Args:
        task_id: ID of the task to update
        new_status: New status to set
        expected_status: Optional status that the task should be in before update
        report: Optional report to update
        
    Returns:
        UpdateStatusAttempt with success status and current status if failed
    """
    
    with SessionBuilder(bind=isolated_engine) as session:
        with session.begin():
            task = cast(Task, session.query(Task).filter(Task.id == task_id).with_for_update().one())
            
            # Check if task is in expected status
            if expected_status is not None and task.status != expected_status.value:
                return UpdateStatusAttempt(
                    is_success=False,
                    current_status=TaskStatus(task.status)
                )

            task.is_cancelled = True

            return UpdateStatusAttempt(is_success=True)

