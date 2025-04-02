from faststream.rabbit import TestRabbitBroker
import pytest

from grader.checking.checking import CheckType
from grader.faststream_tasks.schemes import CheckingTask
from grader.faststream_tasks.tasks import broker

@pytest.mark.asyncio
async def test_handle():
    async with TestRabbitBroker(broker) as br:
        await br.publish(
            CheckingTask(task_uid="1", check_type=CheckType.CLICKHOUSE, args={
                "host": "localhost", 
                "user": "admin", 
                "password": "admin", 
                "student_username": "student", 
                "cluster_name": "main_cluster"
            }), 
            queue="test-queue"
        )

