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
