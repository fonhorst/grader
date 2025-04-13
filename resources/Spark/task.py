import ast
from typing import Union, Callable, Dict, Optional, List, Any

from pyspark.errors import SparkRuntimeException

from check_functions import compare_dataframes
import nbconvert
import nbformat
from pyspark.sql.utils import AnalysisException, PythonException


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

    def test_func(self,
                  func: Callable,
                  func_args: Dict[str, str],
                  result: dict,
                  n_rows: int = 20) -> None:

        print(f"Checking task {self.task_name}...")

        if func:

            gold_result = self.gold_func(**func_args)

            try:

                student_result = func(**func_args)

                if self.check_func:
                    if self.check_func_args is None:
                        self.check_func_args = dict()

                    self.check_func(student_result, gold_result, **self.check_func_args)

                else:

                    compare_dataframes(
                        student=student_result,
                        gold=gold_result,
                        filter_expr=self.filter_expr,
                        sort_df_by=self.sort_df_by,
                        ascending=self.sort_df_ascending,
                        n_rows=n_rows
                    )

                result[self.task_name] = {
                    "is_completed": True,
                    "reason": None
                }

            except (AssertionError, NameError, AnalysisException, SyntaxError,
                    PythonException, TypeError, KeyError, AttributeError, SparkRuntimeException) as e:
                result[self.task_name] = {
                    "is_completed": False,
                    "reason": str(e)
                }
        else:

            if result[self.task_name]["reason"] is None:
                result[self.task_name] = {
                    "is_completed": False,
                    "reason": "Function not found"
                }

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

        # TODO: Check blacklist in list comprehensions, and check .collect() and .toPandas()

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
                        print(f"WARN [{self.task_name}]: {e}")

                    if expr_name in self.expr_blacklist:
                        self.raise_err(f"Prohibited expression call detected: {expr_name}")

        return True


def retrieve_ast_tree_from_notebook(nb_path: str) -> ast.Module:
    nb = nbformat.read(nb_path, as_version=4)

    src_code = preprocess_src_code(
        nbconvert.PythonExporter().from_notebook_node(nb)[0]
    )
    return ast.parse(src_code)


def preprocess_src_code(code: str):
    result = list()

    code = code.replace("\\\n", "")

    for line in code.split("\n"):
        cleaned_line = line.strip()
        if cleaned_line.startswith("!") or cleaned_line.startswith("%"):

            continue
        result.append(line)
    r = "\n".join(result)

    return r


def functions_only(tree):
    return list(filter(
        lambda l: type(l).__name__ == "FunctionDef",
        tree.body
    ))
