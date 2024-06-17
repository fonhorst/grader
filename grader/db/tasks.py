import datetime
import enum
import logging
import os
import uuid
from typing import Optional, Dict, Union, Any, List, cast, Tuple

from sqlalchemy import create_engine, String, UUID, TIMESTAMP, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker, relationship, joinedload

from grader.env import ENV_VAR_RUNNER_DB_CONN, ENV_VAR_ECHO_DB_QUERY

logger = logging.getLogger(__name__)

DateTimeType = Optional[Union[str, float, datetime.datetime]]

DB_CONN = os.environ.get(ENV_VAR_RUNNER_DB_CONN, 'postgresql://postgres:postgres@localhost:5432/wms')

logger.warning("DB_CONN %s" % DB_CONN)

engine = create_engine(DB_CONN, echo=os.environ.get(ENV_VAR_ECHO_DB_QUERY, "yes") == "yes")
# https://docs.sqlalchemy.org/en/20/orm/sessionF_transaction.html#setting-isolation-for-individual-sessions
isolated_engine = engine.execution_options(isolation_level="REPEATABLE READ")
SessionBuilder = sessionmaker(engine)


class ImpossibleTaskStatusTransition(Exception):
    pass


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

    name: Mapped[str] = mapped_column(String(50), nullable=True)

    requester: Mapped[str] = mapped_column(String(50), nullable=True)

    student: Mapped[str] = mapped_column(String(50), nullable=True)

    project: Mapped[str] = mapped_column(String(50), nullable=True)

    tag: Mapped[str] = mapped_column(String(50), nullable=True)

    task_type: Mapped[str] = mapped_column(String(16), nullable=False)

    job_id: Mapped[str] = mapped_column(String(50), nullable=False)

    priority: Mapped[float] = mapped_column(nullable=True, default=0.0)

    parameters: Mapped[Dict[str, Any]] = mapped_column(nullable=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False)

    status_updated_at: Mapped[datetime.datetime] = mapped_column(nullable=False)

    submit_time: Mapped[datetime.datetime] = mapped_column(nullable=False)

    end_time: Mapped[datetime.datetime] = mapped_column(nullable=True)

    metrics: Mapped[Dict[str, Union[str, int, float, bool]]] = mapped_column(nullable=True)

    # change 'lazy' option as you need on query time
    # see: https://stackoverflow.com/questions/52249870/flask-sqlalchemy-change-lazy-in-different-situations
    reason: Mapped["TaskFailReason"] = relationship(uselist=False, lazy='noload', cascade='all, delete')

    def __repr__(self) -> str:
        return f"Task(id={self.id!r}, name={self.name!r}, status={self.status!r})"


class TaskFailReason(Base):
    __tablename__ = "tasksfailreasons"
    id: Mapped[int] = mapped_column(primary_key=True)
    task_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tasks.id"), nullable=False)
    error_message: Mapped[str] = mapped_column(nullable=False)
    error_full: Mapped[str] = mapped_column(nullable=True)


class TaskStatus(enum.Enum):
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


class TaskType(enum.Enum):
    regular = "regular"
    container = "container"
    # spark on k8s as a batch task
    spark = "spark"


def _standartize_datetime(dt: DateTimeType) -> datetime.datetime:
    if isinstance(dt, str):
        dt = datetime.datetime.fromisoformat(dt)
    elif isinstance(dt, float):
        dt = datetime.datetime.fromtimestamp(dt)
    return dt


def handle_name_like(filters: list, param: Mapped[str], value: Optional[Union[str, List[str]]]):
    if value:
        return

    if isinstance(value, list):
        expr = param.in_(value)
    elif '%' in value:
        expr = param.like(value)
    else:
        expr = (param == value)

    filters.append(expr)


def list_tasks(
        uids: Optional[List[str]] = None,
        name: Optional[Union[str, List[str]]] = None,
        requester: Optional[Union[str, List[str]]] = None,
        student: Optional[Union[str, List[str]]] = None,
        project: Optional[Union[str, List[str]]] = None,
        tag: Optional[Union[str, List[str]]] = None,
        task_types: Optional[List[str]] = None,
        job_ids: Optional[List[str]] = None,
        statusess: Optional[List[str]] = None,
        submit_time: Optional[Tuple[DateTimeType, DateTimeType]] = None,
        include_reason: bool = False) -> List[Task]:
    with SessionBuilder() as session:
        query = session.query(Task)

        filters = []
        if uids:
            filters.append(Task.id.in_([uuid.UUID(uid) for uid in uids]))

        vparams = [
            (name, Task.name),
            (requester, Task.requester),
            (student, Task.student),
            (project, Task.project),
            (tag, Task.tag),
            (task_types, Task.task_type),
            (job_ids, Task.job_id),
            (statusess, Task.status)
        ]
        for value, param in vparams:
            handle_name_like(filters, value, param)

        if submit_time:
            start, end = submit_time
            if start:
                filters.append(Task.submit_time >= _standartize_datetime(start))
            if end:
                filters.append(Task.submit_time <= _standartize_datetime(end))

        if len(filters) > 0:
            query = query.filter(*filters)

        if include_reason:
            query = query.options(joinedload(Task.reason, innerjoin=False))

        return cast(List[Task], query.all())


def get_task(task_id: Union[str, uuid.UUID], include_reason: bool = False) -> Task:
    with SessionBuilder() as session:
        if include_reason:
            task = cast(
                Task,
                session.query(Task).filter(Task.id == task_id).options(joinedload(Task.reason, innerjoin=False)).one()
            )
        else:
            task = cast(Task, session.get(Task, task_id))

        session.query()

    return task


def create_task(
    *,
    uid: Optional[uuid.UUID] = None,
    name: Optional[str] = None,
    task_type: TaskType,
    user_id: Union[str, uuid.UUID],
    project_id: Union[str, uuid.UUID],
    job_id: str,
    priority: float = 0.0,
    parameters: Dict[str, Any],
    submit_time: Optional[datetime.datetime] = None
) -> Task:

    with SessionBuilder() as session:
        dt = datetime.datetime.now()
        task = Task(
            id=uid or uuid.uuid4(),
            name=name,
            task_type=task_type.value,
            user_id=str(user_id) if isinstance(user_id, uuid.UUID) else user_id,
            project_id=str(project_id) if isinstance(project_id, uuid.UUID) else project_id,
            job_id=job_id,
            priority=priority,
            parameters=parameters,
            submit_time=submit_time or dt,
            # TODO: unify with TaskStatus enum
            status=TaskStatus.CREATED.value,
            status_updated_at=submit_time or dt
        )
        session.add(task)
        session.commit()

    # TODO: should we read it again?
    return get_task(uid)


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
                        raise ValueError(f"Cannot change status from {curr_status} to {status}")

                    dt = datetime.datetime.now()
                    task.status = status.value
                    task.status_updated_at = dt

                    if TaskStatus.is_terminal(status):
                        task.end_time = dt

                    break
                except OperationalError:
                    logger.error("Unsuccessful attempt to update task status "
                                 "(task_uid=%s) due to operational exeception. "
                                 "Retry %s of %s" % (task_id, attempt, max_retries), exc_info=True)
                attempt += 1


def report_task_metrics(task_id: Union[str, uuid.UUID], **kwargs):
    with SessionBuilder() as session:
        task: Task = session.get(Task, task_id)
        d = dict(task.metrics) if task.metrics else dict()
        d.update(kwargs)
        task.metrics = d
        session.commit()


def delete_all_tasks():
    with SessionBuilder() as session:
        session.query(Task).delete()
        session.commit()


def report_task_fail_reason(task_id: Union[str, uuid.UUID], error_message: str, error_full: Optional[str] = None):
    with SessionBuilder() as session:
        task = TaskFailReason(
            task_id=task_id,
            error_message=error_message,
            error_full=error_full
        )
        session.add(task)
        session.commit()


def get_task_fail_reason(task_id: Union[str, uuid.UUID]) -> TaskFailReason:
    with SessionBuilder() as session:
        reason = cast(TaskFailReason, session.query(TaskFailReason).filter(TaskFailReason.task_id == task_id).one())

    return reason
