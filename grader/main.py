import logging

from grader.app import app
from grader.tasks.app import make_app

# pylint: disable=unused-import
# noinspection PyUnresolvedReferences
from grader.api.endpoints import base_api, tasks_api

from grader.api.endpoints.base_api import base_router

app.include_router(base_router)


def main():
    import uvicorn
    make_app()
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True, log_level=logging.DEBUG)


if __name__ == "__main__":
    main()
