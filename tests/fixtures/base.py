import logging
import os
from typing import Optional

import pytest

logger = logging.getLogger(__name__)


# test admin token
@pytest.fixture(scope="session")
def auth_token():
    return "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyVXVpZCI6IjAwMDAwMDAwLTAwMDAtMDAwMC0" \
           "wMDAwLTAwMDAwMDAwMDAwMSIsInVzZXJMb2dpbiI6ImFkbWluIiwiYWNjb3VudFV1aWQiOiJOb25lIiwicHJvam" \
           "VjdFV1aWQiOiIwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMDEiLCJleHAiOjE3ODM1MjAyMjUuO" \
           "DcwMzE0fQ.MKNCH4jWc6z8l_GTG1tNlaoSDN3GU1DouT8uNfLDTCU"


@pytest.fixture(scope="session")
def auth_token_2():
    return "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyVXVpZCI6IjczYTgwY2EzLTE0N2ItNDQ1My1" \
           "iNWFiLTk1YzliOGFlODhlMyIsInVzZXJMb2dpbiI6IlZsYXNvdlNWIiwiYWNjb3VudFV1aWQiOiIxM2I1NGQ0Yy" \
           "05ZjY1LTRhODAtOWU3Ni1jMWE1YjkyMmQ4MWYiLCJwcm9qZWN0VXVpZCI6IjQ1MGYwOGQ3LTZiM2UtNGU1MS1hZ" \
           "mYyLTAxMGUxZmM0OTJiYyIsImV4cCI6MTc5NDM4MDgyMy40NDA0NDN9.QYf1Yoebz3A42JvCYkHdPIGYRrW6lUH" \
           "zworWd1bv4gc"


@pytest.fixture(scope="session")
def network_name(docker_compose_file) -> Optional[str]:
    # TODO: parse the network name out of the file later
    # return "deploy_test_deploy_net"

    network = os.getenv("WMS_COMPOSE_NETWORK", "host")
    return network


