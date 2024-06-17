import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import psycopg2
import pytest
import redis
from rnseism_sdk.db.tasks import delete_all_tasks
from rnseism_sdk.sdk.base import DataStorage
from rnseism_sdk.sdk.data_storage import RedisDataStorage

from tests.utils import get_ip, replace_hostname

logger = logging.getLogger(__name__)


@dataclass
class DBCtx:
    postgresql_url: str
    rabbitmq_url: str
    redis_url: str
    storage_url: str
    result_storage: DataStorage

    @property
    def result_storage_url(self):
        return f"{self.redis_url}/2"

    @property
    def localhost_result_storage_url(self):
        return "redis://localhost:6379/2"

    @property
    def state_storage_url(self):
        return f"{self.redis_url}/3"

    @property
    def session_storage_url(self):
        return f"{self.redis_url}/4"

    @property
    def parameters_storage_url(self):
        return f"{self.redis_url}/5"

    @property
    def external_postgresql_url(self) -> str:
        return replace_hostname(self.postgresql_url, get_ip('node2.bdcl'))

    @property
    def external_rabbitmq_url(self) -> str:
        return replace_hostname(self.rabbitmq_url, get_ip('node2.bdcl'))

    @property
    def external_redis_url(self) -> str:
        return replace_hostname(self.redis_url, get_ip('node2.bdcl'))


def is_redis_responsive(url):
    try:
        client = redis.from_url(url)
        client.ping()
        logger.info("Redis is now responsive")
        return True
    except redis.exceptions.ConnectionError:
        logger.warning(msg="Redis is NOT responsive", exc_info=True)
        return False


def is_rabbitmq_responsive(url):
    celery_cmd_path = os.path.join(os.path.dirname(sys.executable), 'celery')
    if not os.path.exists(celery_cmd_path):
        raise FileNotFoundError(f"Cannot find celery cmd utility to run the check on path {celery_cmd_path}")
    result = subprocess.run(f"{celery_cmd_path} amqp", shell=True, env={'CELERY_BROKER_URL': url})
    if result.returncode == 0:
        logger.info("RabbitMQ is now responsive")
        return True

    logger.warning(msg="RabbitMQ is NOT responsive")
    return False


def is_postgresql_responsive(url):
    try:
        o = urlparse(url)
        # conn = psycopg2.connect(f"host={o.hostname} port={o.port} user=postgres password=postgres dbname=wms")
        conn = psycopg2.connect(f"host={o.hostname} port={o.port} "
                                f"user={o.username} password={o.password} dbname={o.path[1:]}")
        conn.close()
        logger.info("Postgresql is now responsive")
        return True
    except psycopg2.OperationalError:
        logger.warning(msg="Postgresql is NOT responsive", exc_info=True)
        return False


@pytest.fixture(scope="session")
def redis_url(docker_services) -> Optional[str]:
    # port = docker_services.port_for("redis", 6379)
    # external_url = f"redis://{docker_ip}:{port}"
    # url = f"redis://redis:{port}"

    host = os.getenv("WMS_REDIS_HOST", "localhost")
    port = os.getenv("WMS_REDIS_PORT", 6379)# docker_services.port_for("redis", 6379))

    url = f"redis://{host}:{port}"

    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_redis_responsive(url)
    )
    return url


@pytest.fixture(scope="session")
def rabbitmq_url(docker_services) -> Optional[str]:
    # port = docker_services.port_for("rabbitmq", 5672)
    # external_url = f"amqp://guest:guest@{docker_ip}:{port}"
    # url = f"amqp://guest:guest@rabbitmq:{port}"

    host = os.getenv("WMS_RABBITMQ_HOST", "localhost")
    port = os.getenv("WMS_RABBITMQ_PORT", 5672)#docker_services.port_for("rabbitmq", 5672))

    url = f"amqp://guest:guest@{host}:{port}"

    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_rabbitmq_responsive(url)
    )
    return url


@pytest.fixture(scope="session")
def postgresql_url(docker_services, docker_setup) -> Optional[str]:
    # port = docker_services.port_for("postgresql", 5432)
    # external_url = f"postgresql://postgres:postgres@{docker_ip}:{port}/wms"
    # url = f"postgresql://postgres:postgres@postgresql:{port}/wms"

    host = os.getenv("WMS_POSTGRES_HOST", "localhost")
    port = os.getenv("WMS_POSTGRES_PORT", 5432)#docker_services.port_for("postgresql", 5432))
    user = os.getenv("WMS_POSTGRES_USER", "postgres")
    pwd = os.getenv("WMS_POSTGRES_PWD", "postgres")
    db_name = os.getenv("WMS_POSTGRES_DB", "wms")

    url = f"postgresql://{user}:{pwd}@{host}:{port}/{db_name}"

    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_postgresql_responsive(url)
    )

    if not docker_setup:
        logger.warning("Cleaning postgresql tasks table")
        delete_all_tasks()

    logger.info("Postgresql is prepared")

    return url


@pytest.fixture(scope="session")
def storage_url() -> Optional[str]:
    # TODO: implement accessability checking
    return "10.32.0.243:8095"


@pytest.fixture(scope='session')
def db_ctx(postgresql_url, rabbitmq_url, redis_url, storage_url) -> DBCtx:
    result_storage = RedisDataStorage(prefix="", redis_client=redis.from_url("redis://localhost:6379/2"))
    return DBCtx(
        postgresql_url=postgresql_url,
        rabbitmq_url=rabbitmq_url,
        redis_url=redis_url,
        storage_url=storage_url,
        result_storage=result_storage
    )
