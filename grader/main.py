import logging

from fastapi_pagination import add_pagination
from geowsm.tasks.app import make_app

from geowsm.app import app, jsonrpc_api_v1


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)")


# TODO: importing is needed to bind all required api methods into api_v1 and api_v1_tasks
# TODO: remake it later
# pylint: disable=unused-import
# noinspection PyUnresolvedReferences
from geowsm.api.endpoints import (
    iworkers_api,
    network_storage_api,
    node_api,
    tasks_api,
    codegen_api
)

from geowsm.api.endpoints.base_api import base_router

app.include_router(base_router)

add_pagination(jsonrpc_api_v1)
app.bind_entrypoint(jsonrpc_api_v1)


def main():
    import uvicorn
    celery_app = make_app()

    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True, log_level=logging.DEBUG)


if __name__ == "__main__":
    main()
