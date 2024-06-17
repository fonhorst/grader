import os

from typing import Optional

from pyspark.sql import SparkSession

from rnseism_sdk.utils import PerfomanceLogger
from rnseism_sdk.local_runner import local_runner
from rnseism_sdk.sdk.base import SimpleJob, JobContext

from rnseism_sdk.sdk.pipeline_codegen.spark_2.codegen import (get_subwf_funcs, union_multiple_dfs, init_spark,
                                                              StepImporter, operations, apply_squash_op)
from rnseism_sdk.spark.base import Reorder
from rnseism_sdk.spark.io import VolumeIO, GatherIO, GatherIOOptimized

BLOCKS = {'0': {'0_BlockOp': b'\x80\x04\x95U\x02\x00\x00\x00\x00\x00\x00\x8c\x18network'
                             b'x.classes.digraph\x94\x8c\x07DiGraph\x94\x93\x94)\x81'
                             b'\x94}\x94(\x8c\x05graph\x94}\x94\x8c\x05_node\x94}\x94'
                             b'(\x8c\x01W\x94}\x94(\x8c\x02op\x94\x8c\x0fGatherWriteSt'
                             b'ep\x94\x8c\x06kwargs\x94}\x94(\x8c\x04uuid\x94\x8c d4ed'
                             b'5a0c62b040f4acbdc0607089f535\x94\x8c\x15datastorage_host_'
                             b'port\x94\x8c\x1010.32.0.243:8095\x94\x8c\toverwrite\x94'
                             b'\x88u\x8c\x04type\x94\x8c\x05write\x94u\x8c\x04R.In'
                             b'\x94}\x94(h\x0b\x8c\x02In\x94h\r}\x94(\x8c\tborder_id\x94'
                             b'\x8c\x01R\x94h\x14h\x18u\x8c\x08group_by\x94]\x94(\x8c'
                             b'\x06INLINE\x94\x8c\x05XLINE\x94eh\x14N\x8c\x05level\x94'
                             b'Nuu\x8c\x04_adj\x94}\x94(h\t}\x94h\x16}\x94h\t}\x94(h\x14'
                             b'\x8c\x03arg\x94\x8c\x05order\x94K\x00usu\x8c\x05_su'
                             b'cc\x94h"\x8c\x05_pred\x94}\x94(h\t}\x94h\x16h%sh\x16}'
                             b'\x94u\x8c\x05nodes\x94\x8c\x1cnetworkx.classes.reportv'
                             b'iews\x94\x8c\x08NodeView\x94\x93\x94)\x81\x94}\x94\x8c'
                             b'\x06_nodes\x94h\x08sb\x8c\tin_degree\x94h.\x8c\x0cInDe'
                             b'greeView\x94\x93\x94)\x81\x94}\x94(\x8c\x06_grap'
                             b'h\x94h\x03h(h"h)h*h3h"\x8c\x07_weight\x94Nub\x8c\nout_d'
                             b'egree\x94h.\x8c\rOutDegreeView\x94\x93\x94)\x81\x94}\x94('
                             b'h9h\x03h(h"h)h*h3h"h:Nubub.'}}

UPPER_OPS = b'\x80\x04}\x94.'


class SparkTestJob(SimpleJob):
    def run(self, ctx: JobContext, session_id: Optional[str] = None, **kwargs):
        init_spark()
        custom_ops = StepImporter.import_custom_spark_level_ops()

        with PerfomanceLogger(message="Generated spark job"):
            with operations(ctx, custom_ops, UPPER_OPS, BLOCKS) as (uops, lops):
                # ================= Spark Jobs =================
                # ================= Job: 0 =================
                df_R = GatherIO.read(ctx=ctx, uuid='c04460317c094097ada6d58f86cf8946', filters={
                    '@headers': {'INLINE': {'$gte': 1009, '$lte': 1025}, 'XLINE': {'$gte': 4936, '$lte': 5001}}},
                                     tunings={'headers': ['INLINE', 'XLINE', 'SP_RECNB', 'ACQ_CHN', 'SP_NB', 'CRL_ND',
                                                          'CDP_TRACE', 'OFF_NB', 'RCV_Z', 'SP_Z'],
                                              'include': ['samples']}, navigation={}, group_by=['INLINE', 'XLINE'],
                                     order_by=['INLINE', 'XLINE', 'SP_RECNB'], datastorage_host_port='10.32.0.243:8095')
                df_0_BlockOp = df_R.mapInPandas(**lops.id2subwf['0']['0_BlockOp'].prepare('', [df_R]))

                df_0_BlockOp.write.format('noop').mode('overwrite').save()

        print(f"Execution of GeneratedJob has been finished. Output uuids: {lops.output_uuids()}")

        return lops.task_out()


if __name__ == '__main__':
    local_runner(SparkTestJob())
