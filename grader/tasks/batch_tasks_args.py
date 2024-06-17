from typing import Optional, Dict, List, Any

from rnseism_sdk.db.tasks import TaskType

from geowsm.tasks.base import TaskRunArgs


class BatchTaskRunArgs(TaskRunArgs):
    task_type: str = TaskType.batch.value
    image: str
    environment: Optional[Dict[str, str]] = None
    command: Optional[List[str]] = None
    entrypoint: Optional[str] = None
    cpu: Optional[int] = None
    memory: Optional[int] = None
    volumes: Optional[Dict[str, Any]] = None


class SparkOnK8sBatchTaskRunArgs(BatchTaskRunArgs):
    task_type: str = TaskType.spark.value
    k8s_spark_config_map_name: str = "spark-conf"
    service_type: str = 'ClusterIP'
    service_ports: List[int] = [4040, 39951, 39570]
    service_account_name: str = "spark"
