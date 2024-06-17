from typing import Optional

import apache_beam as beam
from rnseism.sdk.api.clients import CreateVolumeProps
from rnseism.sdk.api.headers import Headers, HeadersColumn
from rnseism.sdk.api.volume import Volume
from rnseism_sdk.ops.transformers import PrintRowCompactRepr, RegisterResult
from rnseism_sdk.ops.volumes import ReadVolumes, VolumeRow, WriteVolumes, ReadVolumesFromUuids
from rnseism_sdk.sdk.base import SimpleJob, JobContext


def generate_volume(trace_id_offset: int, trace_size: int = 100):
    return [
        # Помимо заголовков, также должны быть переданы x и y.
        {
            "trace_id": trace_id_offset + i,
            "xline": i // 10, "inline": i + 0, "header1": 0, "header2": 0.1, "header3": 0.2,
            "x": float(i // 10), "y": i + 0.,
            "trace": [0.1 for _ in range(0, trace_size)]
        }
        for i in range(100)
    ]

def fill_volume(x: VolumeRow, trace_size: int) -> VolumeRow:
    data = generate_volume(100, trace_size=trace_size)
    return x.copy(data)


class PlainVolumeClientReadJob(SimpleJob):
    def run(self, ctx: JobContext, session_id: Optional[str] = None, **kwargs):
        # List Volumes
        volumes = Volume.list(client=ctx.storage.json_rpc_api, filters={}, tunings={}, navigation={})

        return [vol.uuid for vol in volumes]

class ExternalClientsWriteVolumeJob(SimpleJob):
    def run(self, ctx: JobContext, session_id: Optional[str] = None, **kwargs):
        trace_size: int = kwargs['trace_size']

        columns = {
            "xline": HeadersColumn(name="xline", col_type='int', default=0, nullable=False,
                                   indexed=False).to_request(),
            "inline": HeadersColumn(name="inline", col_type='int', default=0, nullable=False,
                                    indexed=False).to_request(),
            "header1": HeadersColumn(name="header1", col_type='float', default=None, nullable=True,
                                     indexed=False).to_request(),
            "header2": HeadersColumn(name="header2", col_type='float', default=None, nullable=True,
                                     indexed=False).to_request(),
            "header3": HeadersColumn(name="test_trace_id", col_type='float', default=None, nullable=True,
                                     indexed=False).to_request()
        }

        obj = Headers.create(
            client=ctx.storage.json_rpc_api,
            name="test_object",
            project_id=1,
            props={"some_prop": 42},
            columns=columns
        )
        headers_uuid = obj.uuid

        # Create new Volume
        volume = Volume.create(
            client=ctx.storage.json_rpc_api,
            name="test",
            project_id=1,
            props=CreateVolumeProps(
                domain="TIME",
                measurement="s",
                data_type="VELOCITY",
                samples_count=trace_size,
                summation_type="IL+XL",
                sample_rate=0.02,
                srd=0.02
            ),
            grid_uuid="dbf08dc7-0d2f-4443-9761-a4e0e4035720",
            headers_uuid=headers_uuid
        )

        with beam.Pipeline() as pipeline:
            written_volumes_uuids = (
                pipeline
                | ReadVolumes(
                    uuids=[volume.uuid],
                    navigation={
                        "order": [
                            {"by": "inline", "asc": True},
                            {"by": "xline", "asc": False}
                        ],
                        "offset": 0,
                        "limit": 100
                    },
                    chunk_size=10,
                    chop_by_order=True,
                    storage=ctx.storage
                )
                | beam.Map(fill_volume, trace_size)
                | WriteVolumes(storage=ctx.storage)
                | beam.Map(lambda x: x[0])
            )

            (
                written_volumes_uuids
                | ReadVolumesFromUuids(
                    tunings={"includeAllHeaders": True},
                    navigation={"limit": 5},
                    storage=ctx.storage
                )
                # sampling and viewing for uuid
                | PrintRowCompactRepr()
            )

            (
                written_volumes_uuids
                | RegisterResult(storage=ctx.storage)
            )

        # volume.remove(force=True)
        # volume.headers.remove(force=True)

        result = ctx.storage.local_in_mem_store()['result']
        return result
