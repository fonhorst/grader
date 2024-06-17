import logging
from typing import Optional

import apache_beam as beam
import numpy as np
from rnseism.sdk.api.headers import Headers
from rnseism_sdk.ops.transformers import RegisterResult
from rnseism_sdk.ops.volumes import ReadVolumes, VolumeRow
from rnseism_sdk.sdk.base import SimpleJob, JobContext

logging.basicConfig(level=logging.INFO)


def process_data(vslice: VolumeRow) -> float:
    avg = np.mean([element for record in vslice.data for element in record['trace']]).astype('float32')
    avg = float(avg) if not np.isnan(avg) else 0.0
    return avg


class ReadVolumesAndReturnValueJob(SimpleJob):
    def run(self, ctx: JobContext, session_id: Optional[str] = None,  **kwargs):
        uuid: str = kwargs['uuid']

        with beam.Pipeline() as pipeline:
            (
                pipeline
                | ReadVolumes(
                    uuids=[uuid],
                    navigation={
                        "order": [
                            {"by": "inline", "asc": True},
                            {"by": "xline", "asc": False}
                        ],
                        "offset": 0,
                        "limit": 100
                    },
                    chunk_size=10,
                    storage=ctx.storage
                )
                | beam.Map(process_data)
                | RegisterResult(storage=ctx.storage)
            )

        result = ctx.storage.local_in_mem_store()['result']

        return result


class PlainHeadersClientReadJob(SimpleJob):
    def run(self, ctx: JobContext, session_id: Optional[str] = None, **kwargs):
        # List Header objects without filter
        objs = Headers.list(client=ctx.storage.json_rpc_api)

        return [obj.uuid for obj in objs]
