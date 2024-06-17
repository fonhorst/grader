import logging
import os
from contextlib import asynccontextmanager

import fastapi_jsonrpc as jsonrpc
from fastapi import Depends, FastAPI
from fastapi.security import HTTPBearer

from geowsm.env import JSONRPC_API_ROOT, ALLOC_MEMORY_MAX_BYTES, ALLOC_USED_MEMORY_RATIO
import cxx_common
import psutil

JSONRPC_SECTION = "Json RPC section"
IWORKER_SECTION = "Management of interpretation service instances"
TASKS_MANAGEMENT_SECTION = "Tasks management"
NODE_MANAGEMENT_SECTION = "Nodes management"
NETWORK_STORAGE_SECTION = "Network storage management"
CODE_GENERATION_SECTION = "Beam pipeline code generation"

logger = logging.getLogger(__name__)

common_errors = []
common_errors.extend(jsonrpc.Entrypoint.default_errors)

# FIXME REMOVE AUTO_ERROR=FALSE
security = HTTPBearer(auto_error=False)


@asynccontextmanager
async def logging_middleware(ctx: jsonrpc.JsonRpcContext):
    logger.info("Request: %r", ctx.raw_request)
    try:
        yield
    finally:
        logger.info("Response: %r", ctx.raw_response)

@asynccontextmanager
async def malloc_trim_middleware(ctx: jsonrpc.JsonRpcContext):
    """
    Выполнение malloc_trim после завершения запросов к API.
    Вызываем освобождение неиспользуемой аллоцированной памяти в случаях:
    1) Если объем выделенной памяти превышает абсолютный объем (ALLOC_MEMORY_MAX_BYTES).
    2) Если объем выделенной памяти превышает реально используемый объем в ALLOC_USED_MEMORY_RATIO раз.
    """
    try:
        yield
    finally:
        process = psutil.Process(os.getpid())
        psutil_mem_info = process.memory_info()
        mallinfo = cxx_common.mallinfo()
        if (ALLOC_MEMORY_MAX_BYTES and (psutil_mem_info.rss >= ALLOC_MEMORY_MAX_BYTES)) or (
                ALLOC_USED_MEMORY_RATIO and (psutil_mem_info.rss >= ALLOC_USED_MEMORY_RATIO * mallinfo.used)):
            logger.info('Mem stats before malloc_trim: %s', psutil_mem_info)
            cxx_common.malloc_trim()
            logger.info('Mem stats after malloc_trim: %s', process.memory_info())


app = FastAPI()
