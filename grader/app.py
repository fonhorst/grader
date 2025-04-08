import logging

from fastapi import FastAPI

from grader.api.tasks_api import router as tasks_router

logger = logging.getLogger(__name__)

app = FastAPI()

app.include_router(tasks_router)

