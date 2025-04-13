import asyncio
import uuid
from datetime import datetime, timedelta
import random
from typing import List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from grader.db.tasks import Task, TaskStatus, AsyncSessionBuilder, create_tables

# Mock data configuration
MOCK_USERS = [
    "student1",
    "student2",
    "student3",
    "student4",
    "student5"
]

MOCK_TAGS = [
    "ClickHouse",
    "Spark",
    "HDFS",
    "Kubernetes",
    "Lab1",
    "Lab2",
    "Lab3",
    "Lab4",
    "Lab5"
]

MOCK_REPORTS = [
    """# Lab Checker Report

## Summary
**Overall Status:** ✅ **PASSED**
**Required Checks:** 5/5 passed
**Optional Checks:** 3/3 passed

## Required Checks That Passed
- Database connection established
- Tables created successfully
- Data loaded correctly
- Queries executed without errors
- Results match expected output

## Optional Checks That Passed
- Performance optimization implemented
- Error handling in place
- Documentation provided""",
    """# Lab Checker Report

## Summary
**Overall Status:** ✅ **PASSED**
**Required Checks:** 4/4 passed
**Optional Checks:** 2/2 passed

## Required Checks That Passed
- Pipeline configuration correct
- Data processing completed
- Output files generated
- Results validated

## Optional Checks That Passed
- Performance metrics within range
- Logging implemented""",
    """# Lab Checker Report

## Summary
**Overall Status:** ✅ **PASSED**
**Required Checks:** 3/3 passed
**Optional Checks:** 2/2 passed

## Required Checks That Passed
- Cluster configuration correct
- Services deployed
- Health checks passed

## Optional Checks That Passed
- Resource utilization optimal
- Monitoring configured"""
]

async def is_database_empty(session: AsyncSession) -> bool:
    """Check if the database is empty by counting tasks."""
    result = await session.execute(select(Task))
    return len(result.scalars().all()) == 0

async def create_mock_tasks(session: AsyncSession) -> List[Task]:
    """Create mock tasks with various statuses and attributes."""
    tasks = []
    now = datetime.now()
    
    # Create 15 tasks
    for i in range(15):
        # Randomly select status, ensuring we have at least 5 finished tasks
        if i < 5:
            status = TaskStatus.FINISHED
        else:
            status = random.choice(list(TaskStatus))
        
        # Randomly select user
        user_id = random.choice(MOCK_USERS)
        
        # Randomly select tag (10 tasks should have tags)
        tag = random.choice(MOCK_TAGS) if i < 10 else None
        
        # Generate random timestamps within the last 30 days
        created_at = now - timedelta(days=random.randint(0, 30))
        updated_at = created_at + timedelta(minutes=random.randint(1, 60))
        
        # Create task
        task = Task(
            id=uuid.uuid4(),
            user_id=user_id,
            name=f"Lab {i+1}",
            tag=tag,
            status=status,
            created_at=created_at,
            updated_at=updated_at,
            report=MOCK_REPORTS[random.randint(0, len(MOCK_REPORTS)-1)] if status == TaskStatus.FINISHED else None
        )
        tasks.append(task)
    
    return tasks

async def init_database():
    """Initialize the database with mock data if empty."""
    async with AsyncSessionBuilder() as session:
        # Check if database is empty
        if await is_database_empty(session):
            # Create tables
            await create_tables()
            
            # Create mock tasks
            tasks = await create_mock_tasks(session)
            
            # Add tasks to session
            for task in tasks:
                session.add(task)
            
            # Commit changes
            await session.commit()
            print("Database initialized with mock data.")
        else:
            print("Database already contains data. Skipping initialization.")