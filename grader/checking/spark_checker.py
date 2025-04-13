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

from grader.checking.base import CheckableQuery, CheckerReport, LabChecker

logger = logging.getLogger(__name__)

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
                 itmo_posts: DataFrame,
                 followers_posts_likes: DataFrame,
                 followers_posts: DataFrame,
                 emojis_data: dict,
                 student_username: str = None):
        self.spark = spark
        self.itmo_posts = itmo_posts
        self.followers_posts_likes = followers_posts_likes
        self.followers_posts = followers_posts
        self.emojis_data = emojis_data
        self.student_username = student_username
        self.checker_report = CheckerReport(checks=[])
        
    def execute_validation_queries(self, validation_queries: List[CheckableQuery]) -> bool:
        """Execute validation queries to check data correctness
        
        Args:
            validation_queries: List of queries to validate
            
        Returns:
            bool: True if all required queries passed
        """
        logger.info("=== Executing validation queries ===")
        
        for query in validation_queries:
            try:
                result = self.spark.sql(query.query)
                if query.validate(result):
                    success_msg = f"Successfully executed query: {query.query}"
                    logger.info(success_msg)
                    self.checker_report.success(
                        description=query.description
                    )
                else:
                    error_msg = f"Query returned no results: {query.query}"
                    logger.error(error_msg)
                    self.checker_report.fail(
                        description=query.description,
                        reason=error_msg
                    )
            except Exception as e:
                error_msg = f"Error executing query: {query.query}\nError: {str(e)}"
                logger.error(error_msg)
                self.checker_report.fail(
                    description=query.description,
                    reason=error_msg,
                    required=True
                )
        
        return True

    def run_checks(self, nb_path: str, skip_tasks: tuple = ()) -> CheckerReport:
        """Run all checks for the Spark lab implementation
        
        Args:
            nb_path: Path to the notebook file
            skip_tasks: Tuple of task names to skip
            
        Returns:
            CheckerReport containing results of all checks
        """
        # Create a fresh report
        self.checker_report = CheckerReport(checks=[])
        
        logger.info(f"Starting checks for student: {self.student_username}")
        
        if not os.path.exists(nb_path):
            error_msg = "ipynb file not found"
            logger.error(error_msg)
            self.checker_report.fail(
                description="Check if notebook exists",
                reason=error_msg,
                required=True
            )
            return self.checker_report

        try:
            student_functions = functions_only(
                retrieve_ast_tree_from_notebook(nb_path)
            )
        except Exception as e:
            error_msg = f"Broken ipynb content: {str(e)}"
            logger.error(error_msg)
            self.checker_report.fail(
                description="Check notebook content",
                reason=error_msg,
                required=True
            )
            return self.checker_report

        ctx = dict()

        # Prepare task arguments
        task1_args = {
            "df": self.itmo_posts,
            "F": F
        }

        TASKS_FUNC_ARGS = {
            "task_1a": task1_args,
            "task_1b": task1_args,
            "task_1c": task1_args,
            "task_2a": {
                "df": self.followers_posts_likes,
                "F": F
            },
            "task_2b": {
                "df": self.followers_posts,
                "F": F
            },
            "task_3": {
                "df": self.followers_posts,
                "F": F
            },
            "task_4": {
                "df": self.itmo_posts,
                "F": F,
                "T": T,
                "emojis_data": self.emojis_data,
                "broadcast_func": self.spark.sparkContext.broadcast
            },
            "task_5": {
                "df": self.followers_posts_likes,
                "F": F,
                "W": Window,
                "top_n_likers": 3
            },
            "task_6": {
                "df": self.followers_posts_likes,
                "F": F,
                "W": Window
            }
        }

        # Check each task
        for func in student_functions:
            func: "ast.FunctionDef"
            task = TASKS.get(func.name, None)
            if task and task.task_name not in skip_tasks:
                try:
                    # Check function source code
                    task.check_function_source_code(func)
                    
                    # Compile and execute function
                    func_code = compile(ast.parse(ast.unparse(func)), f"{task.task_name}", mode="exec")
                    exec(func_code, ctx)
                    
                    # Get function arguments
                    func_args = TASKS_FUNC_ARGS[task.task_name]
                    
                    # Execute student function
                    student_result = ctx[task.func_name](**func_args)
                    
                    # Execute gold function
                    gold_result = task.gold_func(**func_args)
                    
                    # Compare results
                    if task.check_func:
                        task.check_func(
                            student=student_result,
                            gold=gold_result,
                            sort_df_by=task.sort_df_by,
                            sort_df_ascending=task.sort_df_ascending
                        )
                    else:
                        compare_dataframes(
                            student=student_result,
                            gold=gold_result,
                            sort_df_by=task.sort_df_by,
                            sort_df_ascending=task.sort_df_ascending
                        )
                        
                    success_msg = f"Task {task.task_name} completed successfully"
                    logger.info(success_msg)
                    self.checker_report.success(
                        description=success_msg,
                        required=True
                    )
                    
                except ValueError as e:
                    error_msg = str(e)
                    logger.error(f"WARN [{func.name}]: {error_msg}")
                    self.checker_report.fail(
                        description=f"Check {task.task_name} implementation",
                        reason=error_msg,
                        required=True
                    )
                except SyntaxError as e:
                    error_msg = str(e)
                    logger.error(f"WARN [{func.name}]: {error_msg}")
                    self.checker_report.fail(
                        description=f"Check {task.task_name} implementation",
                        reason=error_msg,
                        required=True
                    )
                except AssertionError as e:
                    error_msg = str(e)
                    logger.error(f"WARN [{func.name}]: {error_msg}")
                    self.checker_report.fail(
                        description=f"Check {task.task_name} results",
                        reason=error_msg,
                        required=True
                    )
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"WARN [{func.name}]: {error_msg}")
                    self.checker_report.fail(
                        description=f"Check {task.task_name} execution",
                        reason=error_msg,
                        required=True
                    )

        return self.checker_report 