import pickle
from typing import Optional, Any, Dict

from grader.tasks.base import DataStorage
import redis


class RedisDataStorage(DataStorage):
    def exists(self, result_id: str) -> bool:
        return self._redis.exists(result_id) == 1

    def remove(self, result_id: str):
        self._redis.delete(result_id)

    def list(self, prefix: str) -> Dict[str, Any]:
        return {key: self._redis.get(key) for key in self._redis.keys(prefix)}

    def __init__(self, redis_client: redis.Redis, prefix: Optional[str] = None):
        self._prefix = prefix
        self._redis = redis_client

    def put(self, result_id: str, value: Any):
        pickled_value = pickle.dumps(value)
        self._redis.set(result_id, pickled_value)

    def get(self, result_id: str, default: Optional[Any] = None) -> Any:
        key = result_id
        if default is not None and not self._redis.exists(key):
            return default
        pickled_value = self._redis.get(key)
        value = pickle.loads(pickled_value)

        return value
