from typing import List, Any
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def compare_dataframes(student: "pyspark.sql.dataframe.DataFrame",
                       gold: "pyspark.sql.dataframe.DataFrame",
                       filter_expr: Any = None,
                       sort_df_by: List[str] = None,
                       ascending: List[bool] = None,
                       n_rows: int = 20):

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


def check_task4(student, gold, **kwargs):

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
