import logging
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any, Union

from faststream.rabbit import RabbitBroker
from pydantic import BaseModel

from grader.checking.checking import CheckType
from grader.db.tasks import Course, Student, Task, TaskStatus, create_task, get_task, delete_task, list_tasks, delete_all_tasks, update_task_status_with_isolation, create_course, get_course, update_course, delete_course, list_courses, create_students, get_student, update_student, delete_student, list_students
from grader.faststream_tasks.schemes import CheckingTask

logger = logging.getLogger(__name__)


class CourseInfo(BaseModel):
    id: uuid.UUID
    name: str
    description: Optional[str] = None
    tag: Optional[str] = None   

    @classmethod
    def from_db_course(cls, course: Course) -> 'CourseInfo':
        return cls(
            id=course.id,
            name=course.name,
            description=course.description,
            tag=course.tag
        )
    

class StudentInfo(BaseModel):
    id: uuid.UUID
    name: str
    group: Optional[str] = None
    tag: Optional[str] = None
    course_id: uuid.UUID
    course_name: Optional[str] = None

    @classmethod
    def from_db_student(cls, student: Student) -> 'StudentInfo':
        return cls(
            id=student.id,
            name=student.name,
            group=student.group,
            tag=student.tag,
            course_id=student.course_id,
            course_name=student.course.name
        )


class TaskInfo(BaseModel):
    id: uuid.UUID
    name: str
    tag: Optional[str] = None
    attachment: Optional[str] = None
    status: str
    status_updated_at: datetime
    submit_time: datetime
    end_time: Optional[datetime] = None
    report: Optional[str] = None
    student_id: uuid.UUID
    student_name: Optional[str] = None
    group: Optional[str] = None
    course_id: uuid.UUID
    course_name: Optional[str] = None

    @classmethod
    def from_db_task(cls, task: Task) -> 'TaskInfo':
        return cls(
            id=task.id,
            name=task.name,
            tag=task.tag,
            attachment=task.attachment,
            status=task.status,
            status_updated_at=task.status_updated_at,
            submit_time=task.submit_time,
            end_time=task.end_time,
            report=task.report,
            student_id=task.student_id,
            student_name=task.student.name,
            group=task.student.group,
            course_id=task.student.course_id,
            course_name=task.student.course.name
        ) 


class StudentCourseService:
    async def create_course(
        self,
        *,
        name: str,
        description: Optional[str] = None,
        tag: Optional[str] = None
    ) -> CourseInfo:
        """
        Create a new course.
        
        Args:
            name: Course name
            description: Optional course description
            tag: Optional course tag
            
        Returns:
            Created course info
        """
        course = await create_course(
            name=name,
            description=description,
            tag=tag
        )
        return CourseInfo.from_db_course(course)

    async def get_course(self, course_id: Union[str, uuid.UUID]) -> CourseInfo:
        """
        Get course by ID.
        
        Args:
            course_id: Course ID
            
        Returns:
            Course info
        """
        course = await get_course(course_id)
        return CourseInfo.from_db_course(course)

    async def update_course(
        self,
        course_id: Union[str, uuid.UUID],
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        tag: Optional[str] = None
    ) -> CourseInfo:
        """
        Update course information.
        
        Args:
            course_id: Course ID
            name: Optional new course name
            description: Optional new course description
            tag: Optional new course tag
            
        Returns:
            Updated course info
        """
        course = await update_course(
            course_id=course_id,
            name=name,
            description=description,
            tag=tag
        )
        return CourseInfo.from_db_course(course)

    async def delete_course(self, course_id: Union[str, uuid.UUID]):
        """
        Delete a course.
        
        Args:
            course_id: Course ID
        """
        await delete_course(course_id)

    async def list_courses(
        self,
        *,
        name: Optional[Union[str, List[str]]] = None,
        tag: Optional[Union[str, List[str]]] = None
    ) -> List[CourseInfo]:
        """
        List courses with optional filtering.
        
        Args:
            name: Optional course name filter
            tag: Optional course tag filter
            
        Returns:
            List of course info
        """
        courses = await list_courses(name=name, tag=tag)
        return [CourseInfo.from_db_course(course) for course in courses]

    async def create_students(
        self,
        students_data: List[Dict[str, Any]]
    ) -> List[StudentInfo]:
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
            List of created student info
        """
        students = await create_students(students_data)
        return [StudentInfo.from_db_student(student) for student in students]

    async def get_student(self, student_id: Union[str, uuid.UUID]) -> StudentInfo:
        """
        Get student by ID.
        
        Args:
            student_id: Student ID
            
        Returns:
            Student info
        """
        student = await get_student(student_id)
        return StudentInfo.from_db_student(student)

    async def update_student(
        self,
        student_id: Union[str, uuid.UUID],
        *,
        name: Optional[str] = None,
        group: Optional[str] = None,
        tag: Optional[str] = None,
        course_id: Optional[uuid.UUID] = None
    ) -> StudentInfo:
        """
        Update student information.
        
        Args:
            student_id: Student ID
            name: Optional new student name
            group: Optional new student group
            tag: Optional new student tag
            course_id: Optional new course ID
            
        Returns:
            Updated student info
        """
        student = await update_student(
            student_id=student_id,
            name=name,
            group=group,
            tag=tag,
            course_id=course_id
        )
        return StudentInfo.from_db_student(student)

    async def delete_student(self, student_id: Union[str, uuid.UUID]):
        """
        Delete a student.
        
        Args:
            student_id: Student ID
        """
        await delete_student(student_id)

    async def list_students(
        self,
        *,
        name: Optional[Union[str, List[str]]] = None,
        group: Optional[Union[str, List[str]]] = None,
        tag: Optional[Union[str, List[str]]] = None,
        course_id: Optional[Union[str, List[str]]] = None
    ) -> List[StudentInfo]:
        """
        List students with optional filtering.
        
        Args:
            name: Optional student name filter
            group: Optional student group filter
            tag: Optional student tag filter
            course_id: Optional course ID filter
            
        Returns:
            List of student info
        """
        students = await list_students(
            name=name,
            group=group,
            tag=tag,
            course_id=course_id
        )
        return [StudentInfo.from_db_student(student) for student in students]
    


class CheckerService:
    def __init__(self, queue: str, broker: RabbitBroker):
        self.broker = broker
        self.queue = queue

    async def submit(
        self,
        *,
        student_id: str,
        check_type: CheckType,
        args: Dict[str, Any],
        name: Optional[str] = None,
        tag: Optional[str] = None
    ) -> TaskInfo:
        """
        Submit a new checking task.
        
        Args:
            student_id: ID of the student submitting the task
            check_type: Type of check to perform
            args: Arguments for the checker
            name: Optional name for the task
            tag: Optional tag for the task
            
        Returns:
            Created task response
        """
        # Create task in database with eager loading of student
        task = await create_task(
            uid=uuid.uuid4(),
            name=name or f"Check {check_type.value}",
            student_id=student_id,
            tag=tag
        )
        
        # Create checking task for faststream
        checking_task = CheckingTask(
            task_uid=str(task.id),
            user_id=task.student_id,  # Changed from student_id to task.student_id for consistency
            name=task.name,
            check_type=check_type,
            args=args
        )
        
        # Send task to queue
        await self.broker.publish(checking_task, self.queue)
        logger.info(f"Task {task.id} sent to queue")
        
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
        student_id: Optional[str] = None,
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
            student_id=student_id,
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
