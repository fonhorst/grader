from faststream.rabbit import TestRabbitBroker
import pytest

from grader.checking.checking import CheckType
from grader.faststream_tasks.schemes import CheckingResult, CheckingTask
from grader.faststream_tasks.tasks import broker

@pytest.mark.asyncio
async def test_handle():
    async with TestRabbitBroker(broker) as br:
        print("JUST A TEST")
        # result = await br.publish(
        response = await br.request(
            CheckingTask(task_uid="1", user_id="tutor", name="Test task", check_type=CheckType.CLICKHOUSE, args={
                "host": "localhost", 
                "user": "admin", 
                "password": "admin", 
                "student_username": "student", 
                "cluster_name": "main_cluster"
            }), 
            queue="test-queue"
        )
        result = await response.decode()
        result = CheckingResult.model_validate(result)
        print(f"RESULT: {result}")

