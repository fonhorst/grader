import logging
import os
import subprocess

import pytest
from tests.utils import push_image, IMAGE_TAG

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def docker_registry_repo_url() -> str:
    return "node2.bdcl:5000"


@pytest.fixture(scope='session')
def build_images(pytestconfig):
    logger.info("Building all images")

    proc = subprocess.run(
        "./tests/bin/prepare_test_images.sh",
        shell=True,
        cwd=pytestconfig.rootpath
    )

    if proc.returncode != 0:
        raise RuntimeError(f"Building all images failed")

    logger.info("Finished building all images")


@pytest.fixture(scope="session")
def worker_image(pytestconfig, build_images) -> str:
    image_name = f"iworker:{IMAGE_TAG}"
    # dockerfile_path = os.path.join(pytestconfig.rootpath, 'tests', 'docker', 'worker.dockerfile')

    return image_name


@pytest.fixture(scope="session")
def batch_task_image(pytestconfig, build_images) -> str:
    image_name = f"blauncher_batch_task:{IMAGE_TAG}"
    # dockerfile = os.path.join(pytestconfig.rootpath, 'tests', 'docker', 'blauncher_batch_task.dockerfile')

    return image_name


@pytest.fixture(scope="session")
def pyspark_executor_image(pytestconfig, build_images) -> str:
    image_name = f"pyspark-executor:{IMAGE_TAG}"
    # dockerfile = os.path.join(pytestconfig.rootpath, 'tests', 'docker', 'pyspark_executor.dockerfile')

    return image_name


@pytest.fixture(scope="session")
def main_app_image(pytestconfig, build_images) -> str:
    image_name = f"geowsm:{IMAGE_TAG}"
    # dockerfile_path = os.path.join(pytestconfig.rootpath, 'docker', 'app.dockerfile')

    return image_name


@pytest.fixture(scope='session')
def pushable_worker_image(pytestconfig, docker_registry_repo_url: str, worker_image: str) -> str:
    return push_image(docker_registry_repo_url, worker_image)


@pytest.fixture(scope='session')
def pushable_batch_task_image(pytestconfig, docker_registry_repo_url: str, batch_task_image: str) -> str:
    return push_image(docker_registry_repo_url, batch_task_image)


@pytest.fixture(scope='session')
def pushable_pyspark_executor_image(pytestconfig, docker_registry_repo_url: str, pyspark_executor_image: str) -> str:
    return push_image(docker_registry_repo_url, pyspark_executor_image)


@pytest.fixture(scope='session')
def pushable_main_app_image(pytestconfig, docker_registry_repo_url: str, main_app_image: str) -> str:
    return push_image(docker_registry_repo_url, main_app_image)
