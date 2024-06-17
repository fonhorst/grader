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

BLOCKS = {'0': {'0_BlockOp': b'\x80\x04\x95n\x02\x00\x00\x00\x00\x00\x00\x8c\x18network'
                             b'x.classes.digraph\x94\x8c\x07DiGraph\x94\x93\x94)\x81'
                             b'\x94}\x94(\x8c\x05graph\x94}\x94\x8c\x05_node\x94}\x94'
                             b'(\x8c\x02P2\x94}\x94(\x8c\x02op\x94\x8c\tHMathSte'
                             b'p\x94\x8c\x06kwargs\x94}\x94(\x8c\x07param_a\x94'
                             b'K\x01\x8c\x07param_b\x94G@\x00\x00\x00\x00\x00\x00'
                             b'\x00u\x8c\x05level\x94\x8c\x05chunk\x94u\x8c\x01W\x94}'
                             b'\x94(h\x0b\x8c\x0cTrivialWrite\x94h\r}\x94(\x8c\x03di'
                             b'r\x94\x8c\x05/tmp/\x94\x8c\x05fname\x94\x8c\ntw_out.c'
                             b'sv\x94u\x8c\x04type\x94\x8c\x05write\x94u\x8c\nReorder.In'
                             b'\x94}\x94(h\x0b\x8c\x02In\x94h\r}\x94(\x8c\tborder_id\x94'
                             b'\x8c\x07Reorder\x94h\x1bh\x1fu\x8c\x08group_by\x94]\x94'
                             b'h\x1bNh\x11Nuu\x8c\x04_adj\x94}\x94(h\t}\x94h\x13}\x94(h'
                             b'\x1b\x8c\x03arg\x94\x8c\x05order\x94K\x00ush\x13}\x94h'
                             b'\x1d}\x94h\t}\x94(h\x1bh)h*K\x00usu\x8c\x05_succ\x94h'
                             b'&\x8c\x05_pred\x94}\x94(h\t}\x94h\x1dh-sh\x13}\x94h\th'
                             b'(sh\x1d}\x94u\x8c\x05nodes\x94\x8c\x1cnetworkx.classes.re'
                             b'portviews\x94\x8c\x08NodeView\x94\x93\x94)\x81\x94}\x94'
                             b'\x8c\x06_nodes\x94h\x08sb\x8c\tin_degree\x94h5\x8c\x0cInD'
                             b'egreeView\x94\x93\x94)\x81\x94}\x94(\x8c\x06_graph\x94h'
                             b'\x03h.h&h/h0h:h&\x8c\x07_weight\x94Nub\x8c\nout_degr'
                             b'ee\x94h5\x8c\rOutDegreeView\x94\x93\x94)\x81\x94}\x94(h@h'
                             b'\x03h.h&h/h0h:h&hANubub.'}}

UPPER_OPS = (b'\x80\x04\x95O\x00\x00\x00\x00\x00\x00\x00}\x94\x8c\x02P1\x94}\x94(\x8c\x02o'
             b'p\x94\x8c\x15TrivialUpperProcessor\x94\x8c\x06kwargs\x94}\x94(\x8c\x07para'
             b'm_1\x94K\x04\x8c\x07param_2\x94K\x03uus.')


class SparkTestCustomOpsJob(SimpleJob):
    def run(self, ctx: JobContext, session_id: Optional[str] = None, **kwargs):
        init_spark()
        custom_ops = StepImporter.import_custom_spark_level_ops()

        with PerfomanceLogger(message="Generated spark job"):
            with operations(ctx, custom_ops, UPPER_OPS, BLOCKS) as (uops, lops):
                # ================= Spark Jobs =================
                # ================= Job: 0 =================
                df_R = custom_ops['TrivialMockRead'].read(ctx=ctx, param_1=1, param_2=2)
                df_P1 = df_R.transform(uops.P1)
                df_Reorder = df_P1.transform(Reorder(group_by=['xline', 'inline'], order_by=['xline', 'inline']))
                df_0_BlockOp = df_Reorder.mapInPandas(**lops.id2subwf['0']['0_BlockOp'].prepare('', [df_Reorder]))

                df_0_BlockOp.write.format('noop').mode('overwrite').save()

        print(f"Execution of GeneratedJob has been finished. Output uuids: {lops.output_uuids()}")

        return lops.task_out()


if __name__ == '__main__':
    local_runner(SparkTestCustomOpsJob())
