import os

import pytest

pytest_plugins = [
    "tests.fixtures.app",
    "tests.fixtures.base",
    "tests.fixtures.db",
    "tests.fixtures.images",
    "tests.fixtures.k8s",
    "tests.fixtures.worker"
]

@pytest.fixture(scope="session")
def docker_compose_project_name():
    return ""


@pytest.fixture(scope="session")
def docker_setup():
    return None


@pytest.fixture(scope="session")
def docker_cleanup():
    return None


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootpath), ".devcontainer", "compose.yaml")
    # return os.path.join(str(pytestconfig.rootpath),  "tests", "deploy", "compose.yaml")
