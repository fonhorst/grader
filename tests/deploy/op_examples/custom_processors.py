import logging
import os.path
from typing import List, Callable
from uuid import uuid4

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, IntegerType, StructField, FloatType, ArrayType

from rnseism_sdk.sdk.base import JobContext
from rnseism_sdk.spark.base import Processor, Reader, SquashProcessor
from rnseism_sdk.spark.steps import WriteProcessingStep
from rnseism_sdk.spark.workflow_engine import ProcessingStep


class TrivialUpperProcessor(Processor):
    def processor_func(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.warning(f"TrivialUpperProcessor trace_ids: {list(df['trace_id'])}")
        return df

    def __init__(self, param_1, param_2):
        self.param_1 = param_1
        self.param_2 = param_2
        self.logger = logging.getLogger()


class TrivialSquashProcessor(SquashProcessor):
    def processor_func(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.warning(f"TrivialSquashProcessor df size: {df.shape}")
        return df

    def __init__(self, param_1, param_2):
        self.param_1 = param_1
        self.param_2 = param_2
        self.logger = logging.getLogger()


class TrivialMockRead(Reader):
    @classmethod
    def read(cls, ctx: JobContext, param_1, param_2):
        pdf = pd.DataFrame.from_records([
            [100, .0, 10., [0.1, 0.5, 0.6, 0.9]],
            [101, 1., 11., [0.2, 0.6, 0.7, 1.0]],
            [102, 2., 12., [0.3, 0.7, 0.8, 1.1]],
            [103, 3., 13., [0.4, 0.8, 0.9, 1.2]]
        ], columns=["trace_id", "inline", "xline", "trace"])

        spark = SparkSession.getActiveSession()

        data = spark.createDataFrame(pdf, schema=StructType([
            StructField("trace_id", IntegerType(), nullable=False),
            StructField("inline", FloatType(), nullable=False),
            StructField("xline", FloatType(), nullable=False),
            StructField("trace", ArrayType(FloatType()), nullable=False)
        ]))

        return data


class TrivialWrite(WriteProcessingStep):
    OBJ_TYPE = "fileset"

    def __init__(self, id: str, name: str,  dir, fname):
        self.dir = dir
        self.fname = fname
        self.written_uuid = os.path.join(dir, f"uid_n_{fname}")
        self.id = id
        self.name = name
        self._already_inferred = False

    def get_seismic_obj(self, *args, **kwargs):
        ...

    def __call__(self, *args: pd.DataFrame) -> List[pd.DataFrame]:
        for i, block_df in enumerate(args):
            block_df.to_csv(os.path.join(self.dir, f"{str(uuid4())[:6]}_{i}_{self.fname}"))

        return [pd.DataFrame([])]

    def validate_schema(self, *args, **kwargs):
        ...

    def infer_schema(self, schema: StructType) -> StructType:
        if self._already_inferred:
            raise ValueError("Cannot infer schema twice")
        self._already_inferred = True
        return StructType([])


class CheckInnerGrouping(ProcessingStep):
    def __init__(self, id: str, name: str, grouping: List[str]):
        super().__init__(id, name)
        self.logger = logging.getLogger()
        self.grouping = grouping

    def __call__(self, *args: pd.DataFrame) -> List[pd.DataFrame]:
        msg = "NEW CHECKER CALL:"
        if not self.grouping:
            msg += " empty grouping"
        else:
            for df in args:
                g = df[self.grouping].drop_duplicates()
                for _, slice_df in g.iterrows():
                    msg += f"\n\t {slice_df}"
        self.logger.warning(msg)
        return list(args)

    def infer_schema(self, *schemas: StructType) -> List[StructType]:
        return list(schemas)
