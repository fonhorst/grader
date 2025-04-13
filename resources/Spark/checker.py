import ast
import os
import sys
from typing import Iterable, Optional
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from task import Task, retrieve_ast_tree_from_notebook, functions_only
from gold_functions import *
from check_functions import check_task4

os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_PYTHON"] = sys.executable


COMMON_SIGNATURE = {
    "args": {
        "df": "pyspark.sql.dataframe.DataFrame",
        "F": "pyspark.sql.functions"
    },
    "returns": "pyspark.sql.dataframe.DataFrame"
}


TASKS = {
    "task_1a": Task(# task_name="Top-N posts by likes",
                    task_name="task_1a",
                    func_name="task_1a",
                    func_signature=COMMON_SIGNATURE,
                    gold_func=gold_task_1a,
                    sort_df_by=["likes_count", "post_id"],
                    sort_df_ascending=[False, True]),
    "task_1b": Task(# task_name="Top-N posts by comments",
                    task_name="task_1b",
                    func_name="task_1b",
                    func_signature=COMMON_SIGNATURE,
                    gold_func=gold_task_1b,
                    sort_df_by=["comments_count", "post_id"],
                    sort_df_ascending=[False, True]),
    "task_1c": Task(# task_name="Top-N posts by reposts",
                    task_name="task_1c",
                    func_name="task_1c",
                    func_signature=COMMON_SIGNATURE,
                    gold_func=gold_task_1c,
                    sort_df_by=["reposts_count", "post_id"],
                    sort_df_ascending=[False, True]),
    "task_2a": Task(# task_name="Top-N users by made likes",
                    task_name="task_2a",
                    func_name="task_2a",
                    func_signature=COMMON_SIGNATURE,
                    gold_func=gold_task_2a,
                    sort_df_by=["count", "ownerId"],
                    sort_df_ascending=[False, True]),
    "task_2b": Task(# task_name="Top-N users by made reposts",
                    task_name="task_2b",
                    func_name="task_2b",
                    func_signature=COMMON_SIGNATURE,
                    gold_func=gold_task_2b,
                    sort_df_by=["count", "owner_id"],
                    sort_df_ascending=[False, True]),
    "task_3": Task(# task_name="User post ids from ITMO reposts",
                   task_name="task_3",
                   func_name="task_3",
                   func_signature=COMMON_SIGNATURE,
                   gold_func=gold_task_3,
                   sort_df_by=["reposts_count", "group_post_id"],
                   sort_df_ascending=[False, True]),
    "task_4": Task(# task_name="Top-N posts by reposts",
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
                   sort_df_ascending=[False, True]),
    "task_5": Task(# task_name="Probable fans",
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
                   sort_df_ascending=[True, False, True]),
    "task_6": Task(# task_name="Probable friends",
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
                   sort_df_ascending=[False, True, True]),
}


def get_df_for_tasks(df_path,
                     df_read_func,
                     task_name_list: Iterable[str],
                     ctx: dict,
                     to_cache: bool = False) -> Optional["pyspark.sql.dataframe.DataFrame"]:
    if any(task_name in ctx.keys() for task_name in task_name_list):

        df = df_read_func(df_path)

        if to_cache:
            df = df.cache()

        return df
    else:
        return None


def run(nb_path,
        spark,
        itmo_posts,
        followers_posts_likes,
        followers_posts,
        emojis_data,
        skip_tasks=()):

    if not os.path.exists(nb_path):
        return {
            task: {"is_completed": False, "reason": "ipynb file not found"}
            for task in TASKS.keys()
        }

    results = {
        task: {"is_completed": False, "reason": None}
        for task in TASKS.keys()
    }

    try:
        student_functions = functions_only(
            retrieve_ast_tree_from_notebook(nb_path)
        )
    except Exception as e:
        return {
            task: {"is_completed": False, "reason": f"Broken ipynb content: {str(e)}"}
            for task in TASKS.keys()
        }

    ctx = dict()

    for func in student_functions:
        func: "ast.FunctionDef"
        task = TASKS.get(func.name, None)
        if task:
            try:
                task.check_function_source_code(func)
            except ValueError as e:
                results[task.task_name] = {
                    "is_completed": False,
                    "reason": str(e)
                }
                print(f"WARN [{func.name}]: {e}")
            else:
                try:
                    func_code = compile(ast.parse(ast.unparse(func)), f"{task.task_name}", mode="exec")
                    exec(func_code, ctx)
                except SyntaxError as e:
                    results[task.task_name] = {
                        "is_completed": False,
                        "reason": str(e)
                    }
                    print(f"WARN [{func.name}]: {e}")


    if len(ctx) > 1:

        # if spark is None:
        #     LOCAL_IP = socket.gethostbyname(socket.gethostname())
        #     spark = (
        #         SparkSession
        #         .builder
        #         .appName("test")
        #         .master("local[*]")
        #         #.config("spark.eventLog.enabled", "true")
        #         #.config("spark.eventLog.dir", os.path.join(os.getcwd(), "logs"))
        #         #.config("spark.history.fs.logDirectory", os.path.join(os.getcwd(), "logs"))
        #         .config("spark.driver.host", LOCAL_IP)
        #         #.config("spark.driver.hostname", LOCAL_IP)
        #         .config("spark.driver.bindAddress", "0.0.0.0")
        #         .getOrCreate()
        #     )

        # if datasets is None:
        #     itmo_posts = get_df_for_tasks(
        #         df_path=os.path.join(data_path, "posts_api.json"),
        #         df_read_func=spark.read.json,
        #         task_name_list=("task_1a", "task_1b", "task_1c", "task_4"),
        #         ctx=ctx,
        #         to_cache=False
        #     )
        #
        #     followers_posts_likes = get_df_for_tasks(
        #         df_path=os.path.join(data_path, "followers_posts_likes.parquet"),
        #         df_read_func=spark.read.parquet,
        #         task_name_list=("task_2a", "task_5", "task_6"),
        #         ctx=ctx,
        #         to_cache=False
        #     )
        #
        #     followers_posts = get_df_for_tasks(
        #         df_path=os.path.join(data_path, "followers_posts_api_final.json"),
        #         df_read_func=spark.read.json,
        #         task_name_list=("task_2b", "task_3"),
        #         ctx=ctx,
        #         to_cache=False
        #     )
        # else:

        task1_args = {
            "df": itmo_posts,
            "F": F
        }

        TASKS_FUNC_ARGS = {
            "task_1a": task1_args,
            "task_1b": task1_args,
            "task_1c": task1_args,
            "task_2a": {
                "df": followers_posts_likes,
                "F": F
            },
            "task_2b": {
                "df": followers_posts,
                "F": F
            },
            "task_3": {
                "df": followers_posts,
                "F": F
            },
            "task_4": {
                "df": followers_posts,
                "emojis_data": emojis_data,
                "F": F,
                "T": T,
                "broadcast_func": spark.sparkContext.broadcast
            },
            "task_5": {
                "df": followers_posts_likes,
                "F": F,
                "W": Window,
                "top_n_likers": 10
            },
            "task_6": {
                "df": followers_posts_likes,
                "F": F,
                "W": Window
            }
        }

        print(f"WebUI: {spark.sparkContext.uiWebUrl}")

        for _, task in [
            (task_name, task) for task_name, task in TASKS.items()
            if task_name not in skip_tasks
        ]:
            task.test_func(
                func=ctx.get(task.task_name, None),
                func_args=TASKS_FUNC_ARGS.get(task.task_name, {}),
                result=results,
                n_rows=20
            )

    return results


# if __name__ == "__main__":
#     run(
#         nb_path="data/TestIpynb-2.ipynb",
#         data_path="E:\\VMs\\NCCT_Mint_x64\\_SHARED_DATA\\BigData_Task\\bigdata20\\bigdata20"
#     )







