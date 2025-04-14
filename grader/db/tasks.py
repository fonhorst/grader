import datetime
import enum
import logging
import os
import uuid
from typing import Optional, Dict, Union, Any, List, Tuple

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import ForeignKey, NullPool, String, UUID, TIMESTAMP, delete, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError, IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, subqueryload

from grader.env import ENV_VAR_DB_CONN, ENV_VAR_ECHO_DB_QUERY

logger = logging.getLogger(__name__)

DateTimeType = Optional[Union[str, float, datetime.datetime]]

# Convert connection string to async version
DB_CONN = os.environ.get(ENV_VAR_DB_CONN, 'postgresql://postgres:postgres@localhost:5432/grader')
ASYNC_DB_CONN = DB_CONN.replace('postgresql://', 'postgresql+asyncpg://')

logger.warning("DB_CONN %s" % ASYNC_DB_CONN)

# Create async engine and session maker with proper connection pooling
engine = create_async_engine(
    ASYNC_DB_CONN, 
    echo=os.environ.get(ENV_VAR_ECHO_DB_QUERY, "no") == "yes",
    poolclass=NullPool,
    # Configure pool parameters to help prevent interface errors
    # pool_size=5,
    # max_overflow=10,
    # pool_pre_ping=True,
    # pool_recycle=3600
)

# https://docs.sqlalchemy.org/en/20/orm/sessionF_transaction.html#setting-isolation-for-individual-sessions
isolated_engine = engine.execution_options(isolation_level="REPEATABLE READ")
AsyncSessionBuilder = async_sessionmaker(engine, expire_on_commit=False)

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


class Course(Base):
    __tablename__ = "courses"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)

    name: Mapped[str] = mapped_column(String(50), nullable=False)

    description: Mapped[str] = mapped_column(String(250), nullable=True)

    tag: Mapped[str] = mapped_column(String(50), nullable=True)
    
    students: Mapped[List["Student"]] = relationship(back_populates="course")


class Student(Base):
    __tablename__ = "students"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)

    name: Mapped[str] = mapped_column(String(50), nullable=False)

    group: Mapped[str] = mapped_column(String(50), nullable=True)

    tag: Mapped[str] = mapped_column(String(50), nullable=True)

    course_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("courses.id"), nullable=False)
    
    course: Mapped["Course"] = relationship(back_populates="students")
    
    tasks: Mapped[List["Task"]] = relationship(back_populates="student")


# Task statuses:
# 0 - Created
# 1 - Running
# 2 - Finished
# 3 - Failed
# 4 - Cancelled
class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)

    student_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("students.id"), nullable=False)

    name: Mapped[str] = mapped_column(String(50), nullable=False)

    tag: Mapped[str] = mapped_column(String(50), nullable=True)

    attachment: Mapped[str] = mapped_column(String(50), nullable=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False)

    status_updated_at: Mapped[datetime.datetime] = mapped_column(nullable=False)

    submit_time: Mapped[datetime.datetime] = mapped_column(nullable=False)

    end_time: Mapped[datetime.datetime] = mapped_column(nullable=True)

    report: Mapped[str] = mapped_column(nullable=True)

    is_cancelled: Mapped[bool] = mapped_column(default=False)
    
    student: Mapped["Student"] = relationship(back_populates="tasks")

    def __repr__(self) -> str:
        return f"Task(id={self.id!r}, user_id={self.student_id!r}, name={self.name!r}, status={self.status!r})"


async def create_tables() -> bool:
    """
    Create the tasks table if it doesn't exist.
    Returns True if the table was created, False if it already existed.
    """
    logger.info("Creating required tables")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return True


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
    student_id: str,
    tag: Optional[str] = None,
    attachment: Optional[str] = None,
    submit_time: Optional[datetime.datetime] = None
) -> Task:
    async with AsyncSessionBuilder() as session:
        async with session.begin():
            dt = datetime.datetime.now()
            task_id = uid or uuid.uuid4()
            task = Task(
                id=task_id,
                name=name,
                student_id=student_id,
                tag=tag,
                attachment=attachment,
                status=TaskStatus.CREATED.value,
                status_updated_at=submit_time or dt,
                submit_time=submit_time or dt
            )
            session.add(task)
            await session.flush()  # Ensure the task is saved

            stmt = await session.execute(
                select(Task).where(Task.id == task_id).options(
                    subqueryload(Task.student).subqueryload(Student.course)
                )
            )       
            task = stmt.scalar_one()

            task = await session.get(Task, task_id)

            if not task:
                raise ValueError(f"Failed to create task with ID {task_id}")
            
            return task


async def get_task(task_id: Union[str, uuid.UUID]) -> Task:
    async with AsyncSessionBuilder() as session:
        stmt = await session.execute(
            select(Task).where(Task.id == task_id).options(
                subqueryload(Task.student).subqueryload(Student.course)
            )
        )
        task = stmt.scalar_one_or_none()
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
                        select(Task).where(Task.id == task_id).with_for_update()
                    )
                    task = stmt.scalar_one_or_none()
                    if not task:
                        raise ValueError(f"Task with id {task_id} not found")
                    
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
        async with session.begin():
            task = await session.get(Task, task_id)
            if task:
                await session.delete(task)


async def list_tasks(
        uids: Optional[List[str]] = None,
        name: Optional[Union[str, List[str]]] = None,
        student_id: Optional[Union[str, List[str]]] = None,
        tag: Optional[Union[str, List[str]]] = None,
        statuses: Optional[List[str]] = None,
        submit_time: Optional[Tuple[DateTimeType, DateTimeType]] = None,
        group: Optional[Union[str, List[str]]] = None,
        course_id: Optional[Union[str, List[str]]] = None) -> List[Task]:
    async with AsyncSessionBuilder() as session:
        query = select(Task).join(Task.student).options(
            subqueryload(Task.student).subqueryload(Student.course)
        )

        filters = []
        if uids:
            filters.append(Task.id.in_([uuid.UUID(uid) for uid in uids]))

        vparams = [
            (name, Task.name),
            (student_id, Task.student_id),
            (tag, Task.tag),
            (statuses, Task.status),
            (group, Student.group),
            (course_id, Student.course_id)
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
            query = query.where(*filters)

        result = await session.execute(query)
        return list(result.scalars().all())


async def delete_all_tasks():
    async with AsyncSessionBuilder() as session:
        async with session.begin():
            await session.execute(delete(Task))


async def drop_all_tables():
    async with AsyncSessionBuilder() as session:
        async with session.begin():
            await session.execute(delete(Task))
            await session.execute(delete(Student))
            await session.execute(delete(Course))


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
                        select(Task).where(Task.id == task_id).with_for_update()
                    )
                    
                    task = stmt.scalar_one_or_none()
                    if not task:
                        raise ValueError(f"Task with id {task_id} not found")
                    
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
                    
                    # Commit is handled by the session.begin() context
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


async def create_course(
    *,
    uid: Optional[uuid.UUID] = None,
    name: str,
    description: Optional[str] = None,
    tag: Optional[str] = None
) -> Course:
    async with AsyncSessionBuilder() as session:
        async with session.begin():
            course_id = uid or uuid.uuid4()
            course = Course(
                id=course_id,
                name=name,
                description=description,
                tag=tag
            )
            session.add(course)
        
        course = await session.get(Course, course_id)
        if not course:
            raise ValueError(f"Failed to create course with ID {course_id}")
        return course


async def get_course(course_id: Union[str, uuid.UUID]) -> Course:
    async with AsyncSessionBuilder() as session:
        course = await session.get(Course, course_id)
        if not course:
            raise ValueError(f"Course with id {course_id} not found")
        return course


async def update_course(
    course_id: Union[str, uuid.UUID],
    *,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tag: Optional[str] = None
) -> Course:
    async with AsyncSessionBuilder() as session:
        async with session.begin():
            course = await session.get(Course, course_id)
            if not course:
                raise ValueError(f"Course with id {course_id} not found")
            
            if name is not None:
                course.name = name
            if description is not None:
                course.description = description
            if tag is not None:
                course.tag = tag
            
            return course


async def list_courses(
    *,
    name: Optional[Union[str, List[str]]] = None,
    tag: Optional[Union[str, List[str]]] = None
) -> List[Course]:
    async with AsyncSessionBuilder() as session:
        query = select(Course)
        
        filters = []
        handle_name_like(filters, Course.name, name)
        handle_name_like(filters, Course.tag, tag)
        
        if filters:
            query = query.where(*filters)
            
        result = await session.execute(query)
        return list(result.scalars().all())


async def delete_course(course_id: Union[str, uuid.UUID]):
    """
    Delete a course by ID.
    
    Args:
        course_id: ID of the course to delete
        
    Raises:
        ValueError: If the course has associated students that prevent deletion
    """
    try:
        async with AsyncSessionBuilder() as session:
            async with session.begin():
                course = await session.get(Course, course_id)
                if course:
                    await session.delete(course)
    except IntegrityError as e:
        raise ValueError(
            f"Cannot delete course {course_id} because it has associated students. "
            f"Please delete or move the students first."
        ) from e


async def create_student(
    *,
    uid: Optional[uuid.UUID] = None,
    name: str,
    group: Optional[str] = None,
    tag: Optional[str] = None,
    course_id: uuid.UUID
) -> Student:
    async with AsyncSessionBuilder() as session:
        async with session.begin():
            student_id = uid or uuid.uuid4()
            student = Student(
                id=student_id,
                name=name,
                group=group,
                tag=tag,
                course_id=course_id
            )
            session.add(student)
        
        stmt = await session.execute(
            select(Student).where(Student.id == student_id).options(
                subqueryload(Student.course)
            )
        )
        student = stmt.scalar_one()
        if not student:
            raise ValueError(f"Failed to create student with ID {student_id}")
        return student


async def get_student(student_id: Union[str, uuid.UUID]) -> Student:
    async with AsyncSessionBuilder() as session:
        stmt = await session.execute(
            select(Student).where(Student.id == student_id).options(
                subqueryload(Student.course)
            )
        )
        student = stmt.scalar_one_or_none()
        if not student:
            raise ValueError(f"Student with id {student_id} not found")
        return student


async def update_student(
    student_id: Union[str, uuid.UUID],
    *,
    name: Optional[str] = None,
    group: Optional[str] = None,
    tag: Optional[str] = None,
    course_id: Optional[uuid.UUID] = None
) -> Student:
    async with AsyncSessionBuilder() as session:
        async with session.begin():
            # First get the student
            stmt = await session.execute(
                select(Student).where(Student.id == student_id)
            )
            student = stmt.scalar_one_or_none()
            if not student:
                raise ValueError(f"Student with id {student_id} not found")
            
            # Update the student fields
            if name is not None:
                student.name = name
            if group is not None:
                student.group = group
            if tag is not None:
                student.tag = tag
            if course_id is not None:
                student.course_id = course_id
            
            # Flush changes to the database
            await session.flush()
            
            # Expire the student object to ensure we get fresh data
            session.expire(student)
            
            # Re-fetch the student with eager loading to get updated relationships
            stmt = await session.execute(
                select(Student).where(Student.id == student_id).options(
                    subqueryload(Student.course)
                )
            )
            student = stmt.scalar_one()
            return student


async def list_students(
    *,
    name: Optional[Union[str, List[str]]] = None,
    group: Optional[Union[str, List[str]]] = None,
    tag: Optional[Union[str, List[str]]] = None,
    course_id: Optional[Union[str, List[str]]] = None
) -> List[Student]:
    async with AsyncSessionBuilder() as session:
        query = select(Student).options(
            subqueryload(Student.course)
        )
        
        filters = []
        handle_name_like(filters, Student.name, name)
        handle_name_like(filters, Student.group, group)
        handle_name_like(filters, Student.tag, tag)
        
        if course_id is not None:
            if isinstance(course_id, list):
                filters.append(Student.course_id.in_([uuid.UUID(cid) for cid in course_id]))
            else:
                filters.append(Student.course_id == uuid.UUID(course_id))
        
        if filters:
            query = query.where(*filters)
            
        result = await session.execute(query)
        return list(result.scalars().all())


async def delete_student(student_id: Union[str, uuid.UUID]):
    """
    Delete a student by ID.
    
    Args:
        student_id: ID of the student to delete
        
    Raises:
        ValueError: If the student has associated tasks that prevent deletion
    """
    try:
        async with AsyncSessionBuilder() as session:
            async with session.begin():
                student = await session.get(Student, student_id)
                if student:
                    await session.delete(student)
    except IntegrityError as e:
        raise ValueError(
            f"Cannot delete student {student_id} because they have associated tasks. "
            f"Please delete the tasks first."
        ) from e


async def create_students(
    students_data: List[Dict[str, Any]]
) -> List[Student]:
    """
    Create multiple students in a single transaction.
    
    Args:
        students_data: List of dictionaries containing student data.
            Each dictionary should contain:
            - name (str): Student name
            - course_id (uuid.UUID): Course ID
            - group (Optional[str]): Student group
            - tag (Optional[str]): Student tag
            - uid (Optional[uuid.UUID]): Optional student ID
    
    Returns:
        List of created Student objects
    """
    async with AsyncSessionBuilder() as session:
        async with session.begin():
            # First check if all courses exist
            course_ids = {data['course_id'] for data in students_data}
            stmt = select(Course).where(Course.id.in_(course_ids))
            result = await session.execute(stmt)
            existing_courses = {course.id for course in result.scalars().all()}
            
            # Check if any course is missing
            missing_courses = course_ids - existing_courses
            if missing_courses:
                raise ValueError(f"Courses not found: {missing_courses}")
            
            students = []
            for data in students_data:
                student_id = data.get('uid') or uuid.uuid4()
                student = Student(
                    id=student_id,
                    name=data['name'],
                    course_id=data['course_id'],
                    group=data.get('group'),
                    tag=data.get('tag')
                )
                students.append(student)
                session.add(student)
            
            # Get all created students in a new transaction to ensure they're detached properly
            student_ids = [s.id for s in students]
            created_students = await session.execute(
                select(Student)
                .where(Student.id.in_(student_ids))
                .options(subqueryload(Student.course))
            )
            return list(created_students.scalars().all())

