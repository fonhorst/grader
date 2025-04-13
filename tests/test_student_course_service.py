import uuid
import pytest

from grader.services.checker import StudentCourseService

@pytest.mark.asyncio
async def test_course_operations(clean_tasks_table):
    """Test course CRUD operations."""
    service = StudentCourseService()
    
    # 1. Create course
    course = await service.create_course(
        name="Python 101",
        description="Introduction to Python",
        tag="python"
    )
    assert course.name == "Python 101"
    assert course.description == "Introduction to Python"
    assert course.tag == "python"
    
    # 2. Get course
    retrieved_course = await service.get_course(course.id)
    assert retrieved_course.id == course.id
    assert retrieved_course.name == course.name
    
    # 3. Update course
    updated_course = await service.update_course(
        course_id=course.id,
        name="Advanced Python",
        description="Advanced Python Programming",
        tag="advanced"
    )
    assert updated_course.id == course.id
    assert updated_course.name == "Advanced Python"
    assert updated_course.description == "Advanced Python Programming"
    assert updated_course.tag == "advanced"
    
    # 4. List courses
    courses = await service.list_courses()
    assert len(courses) >= 1
    assert any(c.id == course.id for c in courses)
    
    # Test filtering
    filtered_courses = await service.list_courses(tag="advanced")
    assert len(filtered_courses) >= 1
    assert all(c.tag == "advanced" for c in filtered_courses)
    
    # 5. Delete course
    await service.delete_course(course.id)
    
    # Verify course is deleted
    with pytest.raises(ValueError):
        await service.get_course(course.id)


@pytest.mark.asyncio
async def test_student_operations(clean_tasks_table):
    """Test student CRUD operations."""
    service = StudentCourseService()
    
    # 1. Create course first
    course = await service.create_course(
        name="Test Course",
        description="Test Course Description",
        tag="test"
    )
    
    # 2. Create single student
    students = await service.create_students([{
        "name": "John Doe",
        "course_id": course.id,
        "group": "A",
        "tag": "test_student"
    }])
    student = students[0]

    assert student.name == "John Doe"
    assert student.group == "A"
    assert student.tag == "test_student"
    assert student.course_id == course.id
    assert student.course_name == course.name
    
    # 3. Get student
    retrieved_student = await service.get_student(student.id)
    assert retrieved_student.id == student.id
    assert retrieved_student.name == student.name
    
    # 4. Update student
    updated_student = await service.update_student(
        student_id=student.id,
        name="Jane Doe",
        group="B",
        tag="updated_student"
    )
    assert updated_student.id == student.id
    assert updated_student.name == "Jane Doe"
    assert updated_student.group == "B"
    assert updated_student.tag == "updated_student"
    
    # 5. List students
    students = await service.list_students()
    assert len(students) >= 1
    assert any(s.id == student.id for s in students)
    
    # Test filtering
    filtered_students = await service.list_students(group="B")
    assert len(filtered_students) >= 1
    assert all(s.group == "B" for s in filtered_students)
    
    # Test course filtering
    course_students = await service.list_students(course_id=course.id)
    assert len(course_students) >= 1
    assert all(s.course_id == course.id for s in course_students)
    
    # 6. Delete student
    await service.delete_student(student.id)
    
    # Verify student is deleted
    with pytest.raises(ValueError):
        await service.get_student(student.id)


@pytest.mark.asyncio
async def test_bulk_student_creation(clean_tasks_table):
    """Test creating multiple students in a single transaction."""
    service = StudentCourseService()
    
    # Create course
    course = await service.create_course(
        name="Bulk Test Course",
        description="Course for bulk student creation",
        tag="bulk_test"
    )
    
    # Create multiple students
    students_data = [
        {
            "name": f"Student {i}",
            "course_id": course.id,
            "group": "A" if i % 2 == 0 else "B",
            "tag": "bulk_test"
        }
        for i in range(5)
    ]
    
    students = await service.create_students(students_data)
    
    # Verify all students were created
    assert len(students) == 5
    for i, student in enumerate(students):
        assert student.name == f"Student {i}"
        assert student.course_id == course.id
        assert student.group == ("A" if i % 2 == 0 else "B")
        assert student.tag == "bulk_test"
        assert student.course_name == course.name
    
    # Verify students can be retrieved
    for student in students:
        retrieved = await service.get_student(student.id)
        assert retrieved.id == student.id
        assert retrieved.name == student.name


@pytest.mark.asyncio
async def test_student_course_relationship(clean_tasks_table):
    """Test student-course relationship operations."""
    service = StudentCourseService()
    
    # Create two courses
    course1 = await service.create_course(name="Course 1", tag="test")
    course2 = await service.create_course(name="Course 2", tag="test")
    
    # Create student in first course
    students = await service.create_students([{
        "name": "Test Student",
        "course_id": course1.id,
        "group": "A"
    }])
    student = students[0]
    
    assert student.course_id == course1.id
    assert student.course_name == course1.name
    
    # Move student to second course
    updated_student = await service.update_student(
        student_id=student.id,
        course_id=course2.id
    )
    
    assert updated_student.course_id == course2.id
    assert updated_student.course_name == course2.name
    
    # Verify student appears in new course's list
    course2_students = await service.list_students(course_id=str(course2.id))
    assert any(s.id == student.id for s in course2_students)
    
    # Verify student no longer appears in old course's list
    course1_students = await service.list_students(course_id=str(course1.id))
    assert not any(s.id == student.id for s in course1_students)


@pytest.mark.asyncio
async def test_error_handling(clean_tasks_table):
    """Test error handling for invalid operations."""
    service = StudentCourseService()
    
    # Test getting non-existent course
    with pytest.raises(ValueError):
        await service.get_course(uuid.uuid4())
    
    # Test getting non-existent student
    with pytest.raises(ValueError):
        await service.get_student(uuid.uuid4())
    
    # Test updating non-existent course
    with pytest.raises(ValueError):
        await service.update_course(
            course_id=uuid.uuid4(),
            name="Non-existent"
        )
    
    # Test updating non-existent student
    with pytest.raises(ValueError):
        await service.update_student(
            student_id=uuid.uuid4(),
            name="Non-existent"
        )
    
    # Test deleting non-existent course
    await service.delete_course(uuid.uuid4())  # Should not raise
    
    # Test deleting non-existent student
    await service.delete_student(uuid.uuid4())  # Should not raise
    
    # Test creating student with non-existent course
    with pytest.raises(ValueError):
        await service.create_students([{
            "name": "Test Student",
            "course_id": uuid.uuid4(),
            "group": "A"
        }])

