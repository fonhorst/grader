from fastapi import APIRouter, Depends, HTTPException, status, Body
from typing import Optional
import uuid
import logging

from grader.services.checker import StudentCourseService, CourseInfo
from grader.schemes import (
    MessageResponse,
    CourseResponse,
    CourseListResponse,
    CourseCreateRequest,
    CourseUpdateRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/courses", tags=["courses"])


async def get_student_course_service() -> StudentCourseService:
    return StudentCourseService()


def convert_course_info_to_response(course_info: CourseInfo) -> CourseResponse:
    """Convert CourseInfo to CourseResponse."""
    return CourseResponse(
        id=str(course_info.id),
        name=course_info.name,
        description=course_info.description,
        tag=course_info.tag
    )


# Course endpoints
@router.post("/", response_model=CourseResponse, status_code=status.HTTP_201_CREATED)
async def create_course(
    request: CourseCreateRequest = Body(...),
    service: StudentCourseService = Depends(get_student_course_service)
) -> CourseResponse:
    """
    Create a new course.
    """
    try:
        course_info = await service.create_course(
            name=request.name,
            description=request.description,
            tag=request.tag
        )
        return convert_course_info_to_response(course_info)
    except Exception as e:
        logger.error(
            f"Failed to create course. Name: {request.name}, Description: {request.description}, "
            f"Tag: {request.tag}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{course_id}", response_model=CourseResponse)
async def get_course(
    course_id: uuid.UUID,
    service: StudentCourseService = Depends(get_student_course_service)
) -> CourseResponse:
    """
    Get information about a specific course.
    """
    try:
        course_info = await service.get_course(course_id)
        return convert_course_info_to_response(course_info)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Failed to get course. Course ID: {course_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/", response_model=CourseListResponse)
async def list_courses(
    name: Optional[str] = None,
    tag: Optional[str] = None,
    service: StudentCourseService = Depends(get_student_course_service)
) -> CourseListResponse:
    """
    List all courses with optional filtering.
    """
    try:
        course_infos = await service.list_courses(
            name=name,
            tag=tag
        )
        courses = [convert_course_info_to_response(course_info) for course_info in course_infos]
        return CourseListResponse(courses=courses)
    except Exception as e:
        logger.error(
            f"Failed to list courses. Filters: Name: {name}, Tag: {tag}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.put("/{course_id}", response_model=CourseResponse)
async def update_course(
    course_id: uuid.UUID,
    request: CourseUpdateRequest = Body(...),
    service: StudentCourseService = Depends(get_student_course_service)
) -> CourseResponse:
    """
    Update course information.
    """
    try:
        course_info = await service.update_course(
            course_id=course_id,
            name=request.name,
            description=request.description,
            tag=request.tag
        )
        return convert_course_info_to_response(course_info)
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Failed to update course. Course ID: {course_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/{course_id}", response_model=MessageResponse)
async def delete_course(
    course_id: uuid.UUID,
    service: StudentCourseService = Depends(get_student_course_service)
) -> MessageResponse:
    """
    Delete a course.
    """
    try:
        await service.delete_course(course_id)
        return MessageResponse(message="Course deleted successfully")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Failed to delete course. Course ID: {course_id}",
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

