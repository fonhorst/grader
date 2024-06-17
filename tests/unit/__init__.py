import pickle
from typing import Dict, Any, Optional

import redis


def check_stored_values(redis_url: str, desired_key_values: Dict[str, Any], prefix: Optional[str] = None):
    client = redis.from_url(redis_url)

    found_result_keys = set(k.decode() for k in client.keys(pattern=f"{prefix}.*"))
    desired_key_values = {f"{prefix}.{k}" if prefix else k: v for k, v in desired_key_values.items()}
    assert found_result_keys.symmetric_difference(set(desired_key_values.keys())) == set()

    for k, v in desired_key_values.items():
        result_data = pickle.loads(client.get(k))
        assert result_data == v

    client.close()
