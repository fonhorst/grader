from fastapi import APIRouter

base_router = APIRouter()


@base_router.get("/healthz/", tags=["base"], status_code=200)
async def healthz():
    return "Grader"
