import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

logger = logging.getLogger(__name__)


@asynccontextmanager
async def logging_middleware(ctx):
    logger.info("Request: %r", ctx.raw_request)
    try:
        yield
    finally:
        logger.info("Response: %r", ctx.raw_response)


app = FastAPI(middleware=[logging_middleware])
