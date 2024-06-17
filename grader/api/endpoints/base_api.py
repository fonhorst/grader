from fastapi import APIRouter

from grader.settings import settings

base_router = APIRouter()


@base_router.get("/healthz/", tags=["base"], status_code=200)
async def healthz():
    return settings.app.APP_NAME
