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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"

sales_transactions_bronze_df = read_csv(
    spark,
    f"{SOURCE_PATH.rstrip('/')}/sales_transactions_bronze.csv/",
)

products_bronze_df = read_csv(
    spark,
    f"{SOURCE_PATH.rstrip('/')}/products_bronze.csv/",
)

stores_bronze_df = read_csv(
    spark,
    f"{SOURCE_PATH.rstrip('/')}/stores_bronze.csv/",
)

sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

transactions_silver_sql = """
WITH base AS (
    SELECT
        CAST(stb.transaction_id AS STRING) AS transaction_id,
        CAST(stb.store_id AS STRING) AS store_id,
        CAST(stb.product_id AS STRING) AS product_id,
        TO_DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS transaction_date,
        CAST(COALESCE(stb.quantity, 0) AS INT) AS quantity_sold,
        CAST(COALESCE(stb.sale_amount, 0.0) AS DOUBLE) AS total_revenue,
        CASE
            WHEN CAST(COALESCE(stb.quantity, 0) AS DOUBLE) > 0
                THEN CAST(COALESCE(stb.sale_amount, 0.0) AS DOUBLE)
                     / CAST(stb.quantity AS DOUBLE)
            ELSE NULL
        END AS price_per_unit,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        ROW_NUMBER() OVER (
            PARTITION BY
                CAST(stb.transaction_id AS STRING),
                CAST(stb.store_id AS STRING),
                CAST(stb.product_id AS STRING),
                CAST(stb.transaction_time AS TIMESTAMP)
            ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
        ) AS rn
    FROM sales_transactions_bronze stb
    LEFT JOIN products_bronze pb
        ON CAST(stb.product_id AS STRING) = CAST(pb.product_id AS STRING)
    WHERE stb.transaction_id IS NOT NULL
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

transactions_silver_df = spark.sql(transactions_silver_sql)

safe_overwrite_csv(
    transactions_silver_df,
    f"{TARGET_PATH.rstrip('/')}/transactions_silver.csv/",
    run_id,
    s3_client,
)

stores_silver_sql = """
WITH base AS (
    SELECT
        CAST(sb.store_id AS STRING) AS store_id,
        CAST(sb.store_name AS STRING) AS store_name,
        CAST(sb.city AS STRING) AS city,
        CAST(sb.state AS STRING) AS state,
        CAST(sb.store_type AS STRING) AS store_type,
        TO_DATE(sb.open_date) AS open_date,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(sb.store_id AS STRING)
            ORDER BY CAST(sb.store_id AS STRING) DESC
        ) AS rn
    FROM stores_bronze sb
    WHERE sb.store_id IS NOT NULL
)
SELECT
    store_id,
    store_name,
    city,
    state,
    store_type,
    open_date
FROM base
WHERE rn = 1
"""

stores_silver_df = spark.sql(stores_silver_sql)

safe_overwrite_csv(
    stores_silver_df,
    f"{TARGET_PATH.rstrip('/')}/stores_silver.csv/",
    run_id,
    s3_client,
)

products_silver_sql = """
WITH base AS (
    SELECT
        CAST(pb.product_id AS STRING) AS product_id,
        CAST(pb.product_name AS STRING) AS product_name,
        CAST(pb.category AS STRING) AS category,
        CAST(pb.brand AS STRING) AS brand,
        CAST(pb.price AS DOUBLE) AS price,
        CAST(pb.is_active AS BOOLEAN) AS is_active,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(pb.product_id AS STRING)
            ORDER BY CAST(pb.product_id AS STRING) DESC
        ) AS rn
    FROM products_bronze pb
    WHERE pb.product_id IS NOT NULL
)
SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    is_active
FROM base
WHERE rn = 1
"""

products_silver_df = spark.sql(products_silver_sql)

safe_overwrite_csv(
    products_silver_df,
    f"{TARGET_PATH.rstrip('/')}/products_silver.csv/",
    run_id,
    s3_client,
)

job.commit()