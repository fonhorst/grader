import redis

from geowsm.env import REDIS_PASSWORD, REDIS_PORT, REDIS_HOST

redis_client = redis.Redis(
    host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True
)
