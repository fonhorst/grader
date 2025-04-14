from fastapi import APIRouter, Depends, HTTPException, status, Body
from typing import List, Optional
import uuid
import logging

from grader.services.checker import StudentCourseService, StudentInfo
from grader.schemes import (
    MessageResponse,
    StudentCreateRequest,
    StudentListResponse,
    StudentResponse,
    StudentUpdateRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/students", tags=["students"])


async def get_student_course_service() -> StudentCourseService:
    return StudentCourseService()


def convert_student_info_to_response(student_info: StudentInfo) -> StudentResponse:
    """Convert StudentInfo to StudentResponse."""
    return StudentResponse(
        id=str(student_info.id),
        name=student_info.name,
        group=student_info.group,
        tag=student_info.tag,
        course_id=str(student_info.course_id),
        course_name=student_info.course_name
    )


@router.post("/", response_model=List[StudentResponse], status_code=status.HTTP_201_CREATED)
async def create_students(
    request: List[StudentCreateRequest] = Body(...),
    service: StudentCourseService = Depends(get_student_course_service)
) -> List[StudentResponse]:
    """
    Create multiple students.
    """
    try:
        students_data = [
            {
                "name": student.name,
                "course_id": student.course_id,
                "group": student.group,
                "tag": student.tag
            }
            for student in request
        ]
        student_infos = await service.create_students(students_data)
        return [convert_student_info_to_response(student_info) for student_info in student_infos]
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Failed to create students. Request: {request}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{student_id}", response_model=StudentResponse)
async def get_student(
    student_id: uuid.UUID,
    service: StudentCourseService = Depends(get_student_course_service)
) -> StudentResponse:
    """
    Get information about a specific student.
    """
    try:
        student_info = await service.get_student(student_id)
        return convert_student_info_to_response(student_info)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Failed to get student. Student ID: {student_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/", response_model=StudentListResponse)
async def list_students(
    name: Optional[str] = None,
    group: Optional[str] = None,
    tag: Optional[str] = None,
    course_id: Optional[str] = None,
    service: StudentCourseService = Depends(get_student_course_service)
) -> StudentListResponse:
    """
    List all students with optional filtering.
    """
    try:
        student_infos = await service.list_students(
            name=name,
            group=group,
            tag=tag,
            course_id=course_id
        )
        students = [convert_student_info_to_response(student_info) for student_info in student_infos]
        return StudentListResponse(students=students)
    except Exception as e:
        logger.error(
            f"Failed to list students. Filters: Name: {name}, Group: {group}, "
            f"Tag: {tag}, Course ID: {course_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.put("/{student_id}", response_model=StudentResponse)
async def update_student(
    student_id: uuid.UUID,
    request: StudentUpdateRequest = Body(...),
    service: StudentCourseService = Depends(get_student_course_service)
) -> StudentResponse:
    """
    Update student information.
    """
    try:
        student_info = await service.update_student(
            student_id=student_id,
            name=request.name,
            group=request.group,
            tag=request.tag,
            course_id=request.course_id
        )
        return convert_student_info_to_response(student_info)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Failed to update student. Student ID: {student_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/{student_id}", response_model=MessageResponse)
async def delete_student(
    student_id: uuid.UUID,
    service: StudentCourseService = Depends(get_student_course_service)
) -> MessageResponse:
    """
    Delete a student.
    """
    try:
        await service.delete_student(student_id)
        return MessageResponse(message="Student deleted successfully")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Failed to delete student. Student ID: {student_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) 
    
