from contextlib import contextmanager
from dataclasses import dataclass
import logging
import os
import sys
import tempfile
from typing import Any, Generator, List, Optional, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import subprocess
from pathlib import Path
from hdfs import InsecureClient
from urllib.parse import urlparse

from grader.checking.base import CheckerReport, LabChecker
from grader.checking.utils import get_local_script_path

logger = logging.getLogger(__name__)


@dataclass
class DataframeCheckOrder:
    sort_df_by: List[str]
    sort_df_ascending: List[bool]


TASKS = {
    "task_1a": DataframeCheckOrder(
        sort_df_by=["likes_count", "post_id"],
        sort_df_ascending=[False, True]
    ),
    "task_1b": DataframeCheckOrder(
        sort_df_by=["comments_count", "post_id"],
        sort_df_ascending=[False, True]
    ),
    "task_1c": DataframeCheckOrder(
        sort_df_by=["reposts_count", "post_id"],
        sort_df_ascending=[False, True]
    ),
    "task_2a": DataframeCheckOrder(
        sort_df_by=["count", "ownerId"],
        sort_df_ascending=[False, True]
    ),
    "task_2b": DataframeCheckOrder(
        sort_df_by=["count", "owner_id"],
        sort_df_ascending=[False, True]
    ),
    "task_3": DataframeCheckOrder(
        sort_df_by=["reposts_count", "group_post_id"],
        sort_df_ascending=[False, True]
    ),
    "task_4": DataframeCheckOrder(
        sort_df_by=["count", "emoji"],
        sort_df_ascending=[False, True]
    ),
    "task_5": DataframeCheckOrder(
        sort_df_by=["ownerId", "count", "likerId"],
        sort_df_ascending=[True, False, True]
    ),
    "task_6": DataframeCheckOrder(
        sort_df_by=["mutual_likes", "user_a", "user_b"],
        sort_df_ascending=[False, True, True]
    )
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


def check_task4(student: DataFrame, gold: DataFrame, **kwargs) -> None:
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
                 script_path: str,
                 input_data_path: str,
                 gold_data_path: str,
                 output_dir: str,
                 timeout: int = 30,
                 include_logs: bool = True,
                 hdfs_host: Optional[str] = None,
                 hdfs_port: Optional[int] = None):
        """Initialize SparkChecker
        
        Args:
            script_path: Path to student's script (local or HDFS)
            input_data_path: Path to input dataset
            gold_data_path: Path to gold standard data
            output_dir: Directory where student's script will save results
            timeout: Maximum time in seconds to wait for student's script
            include_logs: Whether to include process logs in CheckerReport
            hdfs_host: HDFS host for downloading scripts (optional)
            hdfs_port: HDFS port for downloading scripts (optional)
        """
        self.script_path = script_path
        self.input_data_path = input_data_path
        self.gold_data_path = gold_data_path
        self.output_dir = output_dir
        self.timeout = timeout
        self.include_logs = include_logs
        self.hdfs_host = hdfs_host
        self.hdfs_port = hdfs_port
        self.checker_report = CheckerReport(checks=[])
        
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _spark_session(self) -> Generator[SparkSession, None, None]:
        """Context manager for creating and cleaning up temporary directory"""
        try:
            spark = SparkSession.builder.master("local[1]").getOrCreate("checker")
            yield spark
        finally:
            spark.stop()

    def run_checks(self) -> CheckerReport:
        """Run all checks for the Spark lab implementation
            
        Returns:
            CheckerReport containing results of all checks
        """
        # Create a fresh report
        self.checker_report = CheckerReport(checks=[])
        
        # Get local path to script
        try:
            local_script_path = get_local_script_path(self.script_path, self.hdfs_host, self.hdfs_port)
        except Exception as e:
            self.checker_report.fail(
                description="Check if script exists",
                reason=f"Failed to access script: {str(e)}",
                required=True
            )
            return self.checker_report

        # Check if script exists
        if not os.path.exists(local_script_path):
            self.checker_report.fail(
                description="Check if script exists",
                reason=f"Script not found at {local_script_path}",
                required=True
            )
            return self.checker_report

        with tempfile.TemporaryDirectory(dir=self.output_dir) as temp_dir, \
            self._spark_session() as spark:
            # Run student's script
            process = subprocess.Popen(
                [
                    sys.executable,
                    local_script_path,
                    "--in", self.input_data_path,
                    "--out", temp_dir
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
                    output_file = os.path.join(temp_dir, f"{task_name}.parquet")
                    if not os.path.exists(output_file):
                        self.checker_report.fail(
                            description=f"Check {task_name} output",
                            reason=f"Output file not found: {output_file}",
                            required=True
                        )
                        continue

                    # Read output dataframe
                    try:
                        student_df = spark.read.parquet(output_file)
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
                        gold_df = spark.read.parquet(gold_file)
                    except Exception as e:
                        self.checker_report.fail(
                            description=f"Check {task_name} gold data",
                            reason=f"Failed to read gold file: {str(e)}",
                            required=True
                        )
                        continue

                    # Compare dataframes
                    try:
                        if task_name == "task_4":
                            check_task4(
                                student=student_df,
                                gold=gold_df,
                                sort_df_by=task.sort_df_by,
                                sort_df_ascending=task.sort_df_ascending
                            )
                        else:
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

