from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def flatten_transcript_messages(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df.select(
            "transcript_id",
            "order_id",
            "customer_id",
            "store_id",
            "created_at",
            "product_sku",
            F.explode("messages").alias("msg"),
        )
        .select(
            "transcript_id",
            "order_id",
            "customer_id",
            "store_id",
            "created_at",
            "product_sku",
            F.col("msg.ts").alias("message_ts"),
            F.col("msg.speaker").alias("speaker"),
            F.col("msg.message_id").alias("message_id"),
            F.col("msg.intent").alias("intent"),
            F.col("msg.sentiment_score").alias("sentiment_score"),
            F.col("msg.return_reason_code").alias("return_reason_code"),
            F.col("msg.refund_requested").alias("refund_requested"),
            F.col("msg.text").alias("text"),
        )
    )


def aggregate_returns(flat_df: DataFrame) -> DataFrame:
    return (
        flat_df.groupBy("store_id", "return_reason_code")
        .agg(
            F.count("*").alias("message_count"),
            F.countDistinct("order_id").alias("orders_touched"),
            F.avg("sentiment_score").alias("avg_sentiment"),
            F.sum(F.when(F.col("refund_requested") == True, 1).otherwise(0)).alias("refund_mentions"),
        )
        .orderBy(F.desc("message_count"))
    )
