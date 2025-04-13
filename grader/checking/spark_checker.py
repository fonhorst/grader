#!/usr/bin/env python3
from abc import ABC, abstractmethod
import logging
import ast
import os
import sys
from typing import Iterable, Optional, Dict, Any, List, Tuple, Union, Callable
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import nbconvert
import nbformat
from pyspark.errors import SparkRuntimeException
from pyspark.sql.utils import AnalysisException, PythonException
import subprocess
import time
import signal
from pathlib import Path

from grader.checking.base import CheckableQuery, CheckerReport, LabChecker

logger = logging.getLogger(__name__)

if False:
    import pyspark
    import pyspark.sql.functions
    from typing import Tuple


def gold_task_1a(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions"
) -> "pyspark.sql.dataframe.DataFrame":
    return df.select(
        F.col("id").alias("post_id"),
        F.col("likes.count").alias("likes_count")
    ).orderBy(
        F.col("likes_count"), F.col("post_id"),
        ascending=[False, True]
    )


def gold_task_1b(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions"
) -> "pyspark.sql.dataframe.DataFrame":
    return df.select(
        F.col("id").alias("post_id"),
        F.col("comments.count").alias("comments_count")
    ).orderBy(
        F.col("comments_count"), F.col("post_id"),
        ascending=[False, True]
    )


def gold_task_1c(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions"
) -> "pyspark.sql.dataframe.DataFrame":
    return df.select(
        F.col("id").alias("post_id"),
        F.col("reposts.count").alias("reposts_count")
    ).orderBy(
        F.col("reposts_count"), F.col("post_id"),
        ascending=[False, True]
    )


def gold_task_2a(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions"
) -> "pyspark.sql.dataframe.DataFrame":
    return df.groupBy(
        F.col("ownerId")
    ).count().orderBy(
        F.col("count"), F.col("ownerId"),
        ascending=[False, True]
    )


def gold_task_2b(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions"
) -> "pyspark.sql.dataframe.DataFrame":
    return df.where(
        F.size(F.col("copy_history")) > 0
    ).groupBy(
        F.col("owner_id")
    ).count().orderBy(
        F.col("count"), F.col("owner_id"),
        ascending=[False, True]
    )


def gold_task_3(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions"
) -> "pyspark.sql.dataframe.DataFrame":
    return df.where(
        (F.size(F.col("copy_history")) > 0) & \
        (F.col("copy_history.owner_id").getItem(0) == -94)
    ).select(
        F.col("copy_history.id").getItem(0).alias("group_post_id"),
        F.col("id").alias("user_post_id")
    ).groupBy(F.col("group_post_id")).agg(
        F.array_sort(F.collect_list("user_post_id")).alias("user_post_ids")
    ).select(
        F.col("group_post_id"),
        F.col("user_post_ids"),
        F.size(F.col("user_post_ids")).alias("reposts_count")
    ).orderBy(
        F.col("reposts_count"),
        F.col("group_post_id"),
        ascending=[False, True]
    )


def gold_task_4(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions",
        T: "pyspark.sql.types",
        emojis_data: dict,
        broadcast_func: "spark.sparkContext.broadcast"
) -> 'Tuple["pyspark.sql.dataframe.DataFrame"]':

    import emoji

    emoji_reg_exp = emoji.get_emoji_regexp()

    sentiment_broadcasted = broadcast_func(emojis_data)

    # emoji==2.2.0
    # @F.udf(returnType=T.ArrayType(T.StringType()))
    # def emoji_udf(text_col):
    #     return [v["emoji"] for v in emoji.emoji_list(text_col)]

    # emoji==0.6.0
    @F.udf(returnType=T.ArrayType(T.StringType()))
    def emoji_udf_var1(text_col):
        return [
            match.group()
            for match in emoji_reg_exp.finditer(text_col)
        ]

    # Поскольку либа emoji немного косячна, то в зависимости от того, каким образом
    # извлекать эмодзи из текста, получаются разные результаты. Поэтому проверяется 2 наиболее
    # популярных способа.
    # emoji==0.6.0
    @F.udf(returnType=T.ArrayType(T.StringType()))
    def emoji_udf_var2(text_col):
        if text_col is None:
            return []
        else:
            return [char for char in text_col if char in emoji.UNICODE_EMOJI['en']]

    @F.udf(returnType=T.StringType())
    def get_sentiment(emoji_col):
        return sentiment_broadcasted.value.get(emoji_col, None)

    result_df_vars = list()
    for udf_func in (emoji_udf_var1, emoji_udf_var2):
        df_var = df.where(
            (F.col("text").isNotNull()) & (F.length(F.col("text")) > 0)
        ).select(
            F.explode(udf_func(F.col("text")).alias("emojis").alias("emoji_udf")).alias("emoji"),
            get_sentiment(F.col("emoji")).alias("sentiment")
        ).groupBy(
            F.col("emoji"), F.col("sentiment")
        ).count()

        result_df_vars.append(df_var)

    return tuple(
        [
            [
                df_var.where(
                    F.col("sentiment") == sentiment
                ).select(
                    F.col("emoji"), F.col("count")
                ).orderBy(
                    F.col("count"), F.col("emoji"), ascending=[False, True]
                ) for sentiment in ("positive", "neutral", "negative")
            ]
            for df_var in result_df_vars
        ]
    )


def gold_task_5(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions",
        W: "pyspark.sql.window.Window",
        top_n_likers: int
) -> "pyspark.sql.dataframe.DataFrame":
    return df.where(
        F.col("likerId") != F.col("ownerId")
    ).groupBy(
        F.col("ownerId"), F.col("likerId")
    ).count().withColumn(
        "row_num",
        F.row_number().over(
            W.partitionBy(F.col("ownerId")).orderBy(
                F.col("count").desc(),
                F.col("LikerId").asc()
            )
        )
    ).where(
        F.col("row_num") <= top_n_likers
    ).select(
        F.col("ownerId"), F.col("likerId"), F.col("count")
    ).orderBy(
        F.col("ownerId"), F.col("count"), F.col("likerId"),
        ascending=[True, False, True]
    )


def gold_task_6(
        df: "pyspark.sql.dataframe.DataFrame",
        F: "pyspark.sql.functions",
        W: "pyspark.sql.window.Window"
) -> "pyspark.sql.dataframe.DataFrame":

    dff = df.where(
        F.col("likerId") != F.col("ownerId")
    ).groupBy(
        F.col("likerId"), F.col("ownerId")
    ).count()

    max_cnts = (
        dff
        .select(F.col("likerId"), F.col("count"))
        .groupBy(F.col("likerId")).agg(
            F.max(F.col("count")).alias("max_cnt")
        )
    )

    return (
        dff.alias("df_1").join(
            dff.alias("df_2"),
            [
                F.col("df_1.likerId") == F.col("df_2.ownerId"),
                F.col("df_1.ownerId") == F.col("df_2.likerId")
            ],
            "inner"
        ).where(
            F.col("df_1.likerId") < F.col("df_1.ownerId")
        )
        .join(
            max_cnts.alias("max_cnts_a"),
            F.col("df_1.likerId") == F.col("max_cnts_a.likerId"),
            "inner"
        ).join(
            max_cnts.alias("max_cnts_b"),
            F.col("df_1.ownerId") == F.col("max_cnts_b.likerId"),
            "inner"
        )
        .where(
            (F.col("df_1.count") == F.col("max_cnts_a.max_cnt")) &
            (F.col("df_2.count") == F.col("max_cnts_b.max_cnt"))
        )
        .select(
            F.col("df_1.likerId").alias("user_a"),
            F.col("df_1.ownerId").alias("user_b"),
            F.col("df_1.count").alias("likes_from_a"),
            F.col("df_2.count").alias("likes_from_b")
        ).select(
            "*", (F.col("likes_from_a") + F.col("likes_from_b")).alias("mutual_likes")
        ).orderBy(
            F.col("mutual_likes"), F.col("user_a"), F.col("user_b"),
            ascending=[False, True, True]
        )
    )


class Task:
    def __init__(self,
                 task_name: str,
                 func_name: str,
                 gold_func: Callable,
                 func_signature: Dict[str, str],  # To assert using AST
                 filter_expr: Any = None,
                 sort_df_by: List[str] = None,
                 sort_df_ascending: List[bool] = None,
                 check_func: Callable = None,
                 check_func_args: Dict[str, Any] = None,
                 allowed_imports: Union[tuple, list] = (),
                 expr_blacklist: Union[tuple, list] = ("eval", "__import__", "exec", "compile"),
                 max_inner_func_depth: int = 0):

        self.task_name = task_name
        self.func_name = func_name
        self.func_signature = func_signature
        self.gold_func = gold_func
        self.allowed_imports = allowed_imports
        self.expr_blacklist = expr_blacklist
        self.max_inner_func_depth = max_inner_func_depth
        self.filter_expr = filter_expr
        self.sort_df_by = sort_df_by
        self.sort_df_ascending = sort_df_ascending
        self.check_func = check_func
        self.check_func_args = check_func_args

    def raise_err(self, message: str):
        raise ValueError(f"{message}\nFunc name: {self.func_name}")

    def check_function_source_code(self, func_source_code: "ast.FunctionDef") -> bool:
        for check_func in [self.check_signature, self.check_content]:
            check_func(func_source_code)
        return True

    def check_signature(self, func_source_code: "ast.FunctionDef") -> bool:
        student_func_signature = dict()
        for arg in func_source_code.args.args:
            name = arg.arg
            arg_type = getattr(arg.annotation, "n", getattr(arg.annotation, "id", None))
            student_func_signature[name] = arg_type

        if not student_func_signature == self.func_signature["args"]:
            self.raise_err(f"Unexpected args signature. Expected: {self.func_signature['args']}. "
                          f"Got: {student_func_signature}")

        return_signature = getattr(
            func_source_code.returns, "n", getattr(func_source_code.returns, "id", None)
        )

        if not return_signature == self.func_signature["returns"]:
            self.raise_err(f"Unexpected return signature. Expected: {self.func_signature['returns']}. "
                          f"Got: {return_signature}")

        return True

    def check_content(self, func_source_code: "ast.FunctionDef", current_depth: int = 0) -> bool:
        if current_depth > self.max_inner_func_depth:
            self.raise_err(f"Max inner functions depth is exceeded. Max depth: {self.max_inner_func_depth}. "
                          f"Current depth: {current_depth}")

        body = func_source_code.body

        for element in body:
            element_type = type(element).__name__

            if element_type == "Import":
                element: ast.Import
                for import_statement in element.names:
                    import_module = import_statement.name.split(".")[0]
                    if import_module not in self.allowed_imports:
                        self.raise_err(f"Unallowed import detected. Allowed imports: {self.allowed_imports}. "
                                     f"Got: {import_module}")

            elif element_type == "ImportFrom":
                element: ast.ImportFrom
                if element.module not in self.allowed_imports:
                    self.raise_err(f"Unallowed import detected. Allowed imports: {self.allowed_imports}. "
                                 f"Got: {element.module}")

            elif element_type == "FunctionDef":
                element: ast.FunctionDef
                if not self.check_content(func_source_code=element, current_depth=current_depth+1):
                    return False

            elif element_type in ("Assign", "Expr"):
                element: Union[ast.Assign, ast.Expr]
                element_value_type_name = type(element.value).__name__
                if element_value_type_name == "Call":
                    try:
                        expr_name = element.value.func.id
                    except AttributeError:
                        expr_name = element.value.func.attr
                    if expr_name in self.expr_blacklist:
                        self.raise_err(f"Prohibited expression call detected: {expr_name}")

                elif element_value_type_name == "Lambda":
                    try:
                        expr_name = element.value.body.func.id
                    except AttributeError as e:
                        expr_name = None
                        logger.warning(f"WARN [{self.task_name}]: {e}")

                    if expr_name in self.expr_blacklist:
                        self.raise_err(f"Prohibited expression call detected: {expr_name}")

        return True

def retrieve_ast_tree_from_notebook(nb_path: str) -> ast.Module:
    nb = nbformat.read(nb_path, as_version=4)
    src_code = preprocess_src_code(
        nbconvert.PythonExporter().from_notebook_node(nb)[0]
    )
    return ast.parse(src_code)

def preprocess_src_code(code: str) -> str:
    result = list()
    code = code.replace("\\\n", "")
    for line in code.split("\n"):
        cleaned_line = line.strip()
        if cleaned_line.startswith("!") or cleaned_line.startswith("%"):
            continue
        result.append(line)
    return "\n".join(result)

def functions_only(tree: ast.Module) -> List[ast.FunctionDef]:
    return list(filter(
        lambda l: type(l).__name__ == "FunctionDef",
        tree.body
    ))

COMMON_SIGNATURE = {
    "args": {
        "df": "pyspark.sql.dataframe.DataFrame",
        "F": "pyspark.sql.functions"
    },
    "returns": "pyspark.sql.dataframe.DataFrame"
}

TASKS = {
    "task_1a": Task(
        task_name="task_1a",
        func_name="task_1a",
        func_signature=COMMON_SIGNATURE,
        gold_func=gold_task_1a,
        sort_df_by=["likes_count", "post_id"],
        sort_df_ascending=[False, True]
    ),
    "task_1b": Task(
        task_name="task_1b",
        func_name="task_1b",
        func_signature=COMMON_SIGNATURE,
        gold_func=gold_task_1b,
        sort_df_by=["comments_count", "post_id"],
        sort_df_ascending=[False, True]
    ),
    "task_1c": Task(
        task_name="task_1c",
        func_name="task_1c",
        func_signature=COMMON_SIGNATURE,
        gold_func=gold_task_1c,
        sort_df_by=["reposts_count", "post_id"],
        sort_df_ascending=[False, True]
    ),
    "task_2a": Task(
        task_name="task_2a",
        func_name="task_2a",
        func_signature=COMMON_SIGNATURE,
        gold_func=gold_task_2a,
        sort_df_by=["count", "ownerId"],
        sort_df_ascending=[False, True]
    ),
    "task_2b": Task(
        task_name="task_2b",
        func_name="task_2b",
        func_signature=COMMON_SIGNATURE,
        gold_func=gold_task_2b,
        sort_df_by=["count", "owner_id"],
        sort_df_ascending=[False, True]
    ),
    "task_3": Task(
        task_name="task_3",
        func_name="task_3",
        func_signature=COMMON_SIGNATURE,
        gold_func=gold_task_3,
        sort_df_by=["reposts_count", "group_post_id"],
        sort_df_ascending=[False, True]
    ),
    "task_4": Task(
        task_name="task_4",
        func_name="task_4",
        allowed_imports=("emoji", "emojis", "emosent"),
        max_inner_func_depth=2,
        func_signature={
            "args": {
                "df": "pyspark.sql.dataframe.DataFrame",
                "emojis_data": "dict",
                "F": "pyspark.sql.functions",
                "T": "pyspark.sql.types",
                "broadcast_func": "spark.sparkContext.broadcast"
            },
            "returns": 'Tuple["pyspark.sql.dataframe.DataFrame"]'
        },
        check_func=check_task4,
        gold_func=gold_task_4,
        sort_df_by=["count", "emoji"],
        sort_df_ascending=[False, True]
    ),
    "task_5": Task(
        task_name="task_5",
        func_name="task_5",
        max_inner_func_depth=2,
        func_signature={
            "args": {
                "df": "pyspark.sql.dataframe.DataFrame",
                "F": "pyspark.sql.functions",
                "W": "pyspark.sql.window.Window",
                "top_n_likers": "int"
            },
            "returns": "pyspark.sql.dataframe.DataFrame"
        },
        gold_func=gold_task_5,
        sort_df_by=["ownerId", "count", "likerId"],
        sort_df_ascending=[True, False, True]
    ),
    "task_6": Task(
        task_name="task_6",
        func_name="task_6",
        max_inner_func_depth=2,
        func_signature={
            "args": {
                "df": "pyspark.sql.dataframe.DataFrame",
                "F": "pyspark.sql.functions",
                "W": "pyspark.sql.window.Window"
            },
            "returns": "pyspark.sql.dataframe.DataFrame"
        },
        gold_func=gold_task_6,
        sort_df_by=["mutual_likes", "user_a", "user_b"],
        sort_df_ascending=[False, True, True]
    ),
}

def compare_dataframes(student: DataFrame,
                      gold: DataFrame,
                      filter_expr: Any = None,
                      sort_df_by: List[str] = None,
                      ascending: List[bool] = None,
                      n_rows: int = 20) -> None:
    """Compare two dataframes for equality
    
    Args:
        student: Student's dataframe
        gold: Gold standard dataframe
        filter_expr: Optional filter expression
        sort_df_by: Columns to sort by
        ascending: Sort order for each column
        n_rows: Number of rows to compare
        
    Raises:
        AssertionError: If dataframes don't match
    """
    assert isinstance(student, DataFrame), f"The result is not pyspark DataFrame. Got: {type(student)}"

    assert student.columns == gold.columns, \
        (f"Submitted dataframe and Test dataframe columns are not equal!\n"
         f"Expected: {gold.columns}\n"
         f"Got: {student.columns}\n")

    if filter_expr:
        student = student.where(filter_expr)
        gold = gold.where(filter_expr)

    if sort_df_by:
        sort = [F.col(col_name) for col_name in sort_df_by]
        asc = ascending if ascending else [True for _ in sort_df_by]

        student = student.orderBy(sort, ascending=asc)
        gold = gold.orderBy(sort, ascending=asc)

    assert student.take(n_rows) == gold.take(n_rows), \
        (f"Dataframe from submitted function not equals to Test Dataframe!\n"
         f"Expected:{gold.show(n_rows)} (HIDDEN)\n"
         f"Got: {student.show(n_rows)} (HIDDEN)")

def check_task4(student, gold, **kwargs) -> None:
    """Special check function for task 4 that handles multiple valid solutions
    
    Args:
        student: Student's solution
        gold: Gold standard solution
        **kwargs: Additional arguments passed to compare_dataframes
        
    Raises:
        AssertionError: If solution doesn't match either gold standard
    """
    gold_var1, gold_var2 = gold

    for i, (student_df, gold_df, sentiment) in enumerate(zip(student, gold_var1, ("positive", "neutral", "negative"))):
        try:
            compare_dataframes(
                student=student_df,
                gold=gold_df,
                **kwargs
            )
        except AssertionError as e:
            try:
                compare_dataframes(
                    student=student_df,
                    gold=gold_var2[i],
                    **kwargs
                )
            except AssertionError as e:
                raise AssertionError(f"{e}: sentiment - {sentiment}")


class SparkChecker(LabChecker):
    def __init__(self, 
                 spark: SparkSession,
                 input_data_path: str,
                 gold_data_path: str,
                 output_dir: str,
                 timeout: int = 30,
                 include_logs: bool = True):
        """Initialize SparkChecker
        
        Args:
            spark: SparkSession instance
            input_data_path: Path to input dataset
            gold_data_path: Path to gold standard data
            output_dir: Directory where student's script will save results
            timeout: Maximum time in seconds to wait for student's script
            include_logs: Whether to include process logs in CheckerReport
        """
        self.spark = spark
        self.input_data_path = input_data_path
        self.gold_data_path = gold_data_path
        self.output_dir = output_dir
        self.timeout = timeout
        self.include_logs = include_logs
        self.checker_report = CheckerReport(checks=[])
        
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    def run_checks(self, script_path: str) -> CheckerReport:
        """Run all checks for the Spark lab implementation
        
        Args:
            script_path: Path to student's Python script
            
        Returns:
            CheckerReport containing results of all checks
        """
        # Create a fresh report
        self.checker_report = CheckerReport(checks=[])
        
        # Check if script exists
        if not os.path.exists(script_path):
            self.checker_report.fail(
                description="Check if script exists",
                reason=f"Script not found at {script_path}",
                required=True
            )
            return self.checker_report

        # Run student's script
        process = subprocess.Popen(
            [
                sys.executable,
                script_path,
                "--in", self.input_data_path,
                "--out", self.output_dir
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        try:
            # Wait for process with timeout
            stdout, stderr = process.communicate(timeout=self.timeout)
            exit_code = process.returncode
            
            # Add process logs to report if requested
            if self.include_logs:
                log_msg = f"Script output:\n{stdout}\n\nScript errors:\n{stderr}"
            else:
                log_msg = None

            # Check if script completed successfully
            if exit_code != 0:
                self.checker_report.fail(
                    description="Check script execution",
                    reason=f"Script failed with exit code {exit_code}",
                    details=log_msg,
                    required=True
                )
                return self.checker_report
            else:
                self.checker_report.success(
                    description="Check script execution",
                    details=log_msg,
                    required=True
                )

        except subprocess.TimeoutExpired:
            # Kill the process if it times out
            process.kill()
            self.checker_report.fail(
                description="Check script execution",
                reason=f"Script timed out after {self.timeout} seconds",
                required=True
            )
            return self.checker_report

        # Check each task's output
        for task_name, task in TASKS.items():
            try:
                # Check if output file exists
                output_file = os.path.join(self.output_dir, f"{task_name}.parquet")
                if not os.path.exists(output_file):
                    self.checker_report.fail(
                        description=f"Check {task_name} output",
                        reason=f"Output file not found: {output_file}",
                        required=True
                    )
                    continue

                # Read output dataframe
                try:
                    student_df = self.spark.read.parquet(output_file)
                except Exception as e:
                    self.checker_report.fail(
                        description=f"Check {task_name} output",
                        reason=f"Failed to read output file: {str(e)}",
                        required=True
                    )
                    continue

                # Check if dataframe is not empty
                if student_df.count() == 0:
                    self.checker_report.fail(
                        description=f"Check {task_name} output",
                        reason="Output dataframe is empty",
                        required=True
                    )
                    continue

                # Read gold dataframe
                gold_file = os.path.join(self.gold_data_path, f"{task_name}.parquet")
                try:
                    gold_df = self.spark.read.parquet(gold_file)
                except Exception as e:
                    self.checker_report.fail(
                        description=f"Check {task_name} gold data",
                        reason=f"Failed to read gold file: {str(e)}",
                        required=True
                    )
                    continue

                # Compare dataframes
                try:
                    compare_dataframes(
                        student=student_df,
                        gold=gold_df,
                        sort_df_by=task.sort_df_by,
                        sort_df_ascending=task.sort_df_ascending
                    )
                    self.checker_report.success(
                        description=f"Check {task_name} results",
                        required=True
                    )
                except AssertionError as e:
                    self.checker_report.fail(
                        description=f"Check {task_name} results",
                        reason=str(e),
                        required=True
                    )

            except Exception as e:
                self.checker_report.fail(
                    description=f"Check {task_name}",
                    reason=f"Unexpected error: {str(e)}",
                    required=True
                )

        return self.checker_report 