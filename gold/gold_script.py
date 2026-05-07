import sys
import uuid
import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


def parse_s3_uri(uri):
    clean_uri = uri.replace("s3://", "", 1)
    bucket, _, key = clean_uri.partition("/")
    return bucket, key.rstrip("/") + "/"


def delete_s3_prefix(s3_client, s3_uri):
    bucket, prefix = parse_s3_uri(s3_uri)
    paginator = s3_client.get_paginator("list_objects_v2")

    delete_batch = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            delete_batch.append({"Key": obj["Key"]})

            if len(delete_batch) == 1000:
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": delete_batch})
                delete_batch = []

    if delete_batch:
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": delete_batch})


def copy_s3_prefix(s3_client, source_uri, target_uri):
    source_bucket, source_prefix = parse_s3_uri(source_uri)
    target_bucket, target_prefix = parse_s3_uri(target_uri)

    paginator = s3_client.get_paginator("list_objects_v2")
    copied = 0

    for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
        for obj in page.get("Contents", []):
            source_key = obj["Key"]
            relative_key = source_key[len(source_prefix):]

            if not relative_key:
                continue

            target_key = target_prefix + relative_key
            s3_client.copy_object(
                Bucket=target_bucket,
                Key=target_key,
                CopySource={"Bucket": source_bucket, "Key": source_key},
            )
            copied += 1

    if copied == 0:
        raise RuntimeError(f"No files were written to temporary S3 path: {source_uri}")


def safe_overwrite_csv(df, target_uri, run_id, s3_client):
    """
    Writes to an isolated temp prefix first, then replaces the target prefix.
    This avoids Spark/S3 overwrite commit failures in the final create/swap phase.
    """
    target_uri = target_uri.rstrip("/") + "/"
    temp_uri = target_uri.rstrip("/") + f"__tmp_{run_id}/"

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(temp_uri)
    )

    delete_s3_prefix(s3_client, target_uri)
    copy_s3_prefix(s3_client, temp_uri, target_uri)
    delete_s3_prefix(s3_client, temp_uri)


def read_csv(spark, path):
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path.rstrip("/") + "/")
    )

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_client = boto3.client("s3")
run_id = str(uuid.uuid4()).replace("-", "")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"

transactions_silver_df = read_csv(
    spark,
    f"{SOURCE_PATH.rstrip('/')}/transactions_silver.csv/",
)

stores_silver_df = read_csv(
    spark,
    f"{SOURCE_PATH.rstrip('/')}/stores_silver.csv/",
)

products_silver_df = read_csv(
    spark,
    f"{SOURCE_PATH.rstrip('/')}/products_silver.csv/",
)

transactions_silver_df.createOrReplaceTempView("transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

gold_sales_analytics_sql = """
WITH base AS (
    SELECT
        CAST(ts.transaction_id AS STRING) AS transaction_id,
        CAST(ts.store_id AS STRING) AS store_id,
        CAST(ts.product_id AS STRING) AS product_id,
        TO_DATE(ts.transaction_date) AS transaction_date,
        CAST(ts.quantity_sold AS INT) AS quantity_sold,
        CAST(ts.total_revenue AS DOUBLE) AS total_revenue,
        CAST(ts.price_per_unit AS DOUBLE) AS price_per_unit,
        ROW_NUMBER() OVER (
            PARTITION BY
                CAST(ts.transaction_id AS STRING),
                CAST(ts.store_id AS STRING),
                CAST(ts.product_id AS STRING)
            ORDER BY TO_DATE(ts.transaction_date) DESC
        ) AS rn
    FROM transactions_silver ts
    INNER JOIN stores_silver ss
        ON CAST(ts.store_id AS STRING) = CAST(ss.store_id AS STRING)
    INNER JOIN products_silver ps
        ON CAST(ts.product_id AS STRING) = CAST(ps.product_id AS STRING)
    WHERE ts.transaction_id IS NOT NULL
)
SELECT
    transaction_id,
    store_id,
    product_id,
    transaction_date,
    quantity_sold,
    total_revenue,
    price_per_unit
FROM base
WHERE rn = 1
"""

gold_sales_analytics_df = spark.sql(gold_sales_analytics_sql)

safe_overwrite_csv(
    gold_sales_analytics_df,
    f"{TARGET_PATH.rstrip('/')}/gold_sales_analytics.csv/",
    run_id,
    s3_client,
)

gold_store_performance_sql = """
SELECT
    CAST(ts.store_id AS STRING) AS store_id,
    CAST(SUM(CAST(ts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
    CAST(COUNT(DISTINCT CAST(ts.transaction_id AS STRING)) AS BIGINT) AS num_transactions,
    CAST(
        SUM(CAST(ts.total_revenue AS DOUBLE))
        / NULLIF(COUNT(DISTINCT CAST(ts.transaction_id AS STRING)), 0)
        AS DOUBLE
    ) AS average_transaction_value
FROM transactions_silver ts
INNER JOIN stores_silver ss
    ON CAST(ts.store_id AS STRING) = CAST(ss.store_id AS STRING)
GROUP BY CAST(ts.store_id AS STRING)
"""

gold_store_performance_df = spark.sql(gold_store_performance_sql)

safe_overwrite_csv(
    gold_store_performance_df,
    f"{TARGET_PATH.rstrip('/')}/gold_store_performance.csv/",
    run_id,
    s3_client,
)

gold_product_performance_sql = """
SELECT
    CAST(ts.product_id AS STRING) AS product_id,
    CAST(SUM(CAST(ts.total_revenue AS DOUBLE)) AS DOUBLE) AS total_revenue,
    CAST(SUM(CAST(ts.quantity_sold AS BIGINT)) AS BIGINT) AS units_sold,
    CAST(
        SUM(CAST(ts.total_revenue AS DOUBLE))
        / NULLIF(SUM(CAST(ts.quantity_sold AS BIGINT)), 0)
        AS DOUBLE
    ) AS average_price
FROM transactions_silver ts
INNER JOIN products_silver ps
    ON CAST(ts.product_id AS STRING) = CAST(ps.product_id AS STRING)
GROUP BY CAST(ts.product_id AS STRING)
"""

gold_product_performance_df = spark.sql(gold_product_performance_sql)

safe_overwrite_csv(
    gold_product_performance_df,
    f"{TARGET_PATH.rstrip('/')}/gold_product_performance.csv/",
    run_id,
    s3_client,
)

job.commit()