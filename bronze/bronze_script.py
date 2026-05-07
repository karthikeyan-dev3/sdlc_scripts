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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/src/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"

metadata = {
    "tables": [
        {
            "source_table": "products_raw",
            "target_table": "products_bronze",
            "columns": [
                "product_id",
                "product_name",
                "category",
                "brand",
                "price",
                "is_active",
            ],
        },
        {
            "source_table": "stores_raw",
            "target_table": "stores_bronze",
            "columns": [
                "store_id",
                "store_name",
                "city",
                "state",
                "store_type",
                "open_date",
            ],
        },
        {
            "source_table": "sales_transactions_raw",
            "target_table": "sales_transactions_bronze",
            "columns": [
                "transaction_id",
                "store_id",
                "product_id",
                "quantity",
                "sale_amount",
                "transaction_time",
            ],
        },
    ]
}

for table in metadata["tables"]:
    source_uri = f"{SOURCE_PATH.rstrip('/')}/{table['source_table']}.csv/"
    target_uri = f"{TARGET_PATH.rstrip('/')}/{table['target_table']}.csv/"

    source_df = read_csv(spark, source_uri)

    missing_columns = [column for column in table["columns"] if column not in source_df.columns]
    if missing_columns:
        raise RuntimeError(
            f"Missing required columns in {table['source_table']}: {missing_columns}. "
            f"Available columns: {source_df.columns}"
        )

    target_df = source_df.select(*table["columns"])

    safe_overwrite_csv(target_df, target_uri, run_id, s3_client)

job.commit()