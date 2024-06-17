import logging

from grader.app import app
from grader.tasks.app import make_app

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)")


# pylint: disable=unused-import
# noinspection PyUnresolvedReferences
from geowsm.api.endpoints import (
    iworkers_api,
    network_storage_api,
    node_api,
    tasks_api,
    codegen_api
)

from grader.api.endpoints.base_api import base_router

app.include_router(base_router)


def main():
    import uvicorn
    make_app()
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True, log_level=logging.DEBUG)


if __name__ == "__main__":
    main()
