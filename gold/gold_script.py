import sys
import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
    TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
    FILE_FORMAT = "csv"

    # --------------------------------------------------------------------------------------
    # 1) Read source tables from S3
    # --------------------------------------------------------------------------------------
    sales_transactions_silver_df = (
        spark.read.format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
    )

    product_silver_df = (
        spark.read.format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
    )

    store_silver_df = (
        spark.read.format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
    )

    # --------------------------------------------------------------------------------------
    # 2) Create temp views
    # --------------------------------------------------------------------------------------
    sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
    product_silver_df.createOrReplaceTempView("product_silver")
    store_silver_df.createOrReplaceTempView("store_silver")

    # --------------------------------------------------------------------------------------
    # Target: gold_sales_transactions
    # --------------------------------------------------------------------------------------
    gold_sales_transactions_df = spark.sql(
        """
        SELECT
            CAST(sts.transaction_id AS STRING) AS sale_id,
            CAST(sts.transaction_time AS TIMESTAMP) AS sale_datetime,
            DATE(CAST(sts.transaction_time AS TIMESTAMP)) AS sale_date,
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(sts.quantity AS INT) AS quantity_sold,
            CAST(ps.price AS FLOAT) AS unit_price,
            CAST((CAST(sts.quantity AS DOUBLE) * CAST(ps.price AS DOUBLE)) AS DOUBLE) AS gross_sales_amount,
            CAST(0.0 AS DOUBLE) AS discount_amount,
            CAST((CAST(sts.quantity AS DOUBLE) * CAST(ps.price AS DOUBLE)) - 0.0 AS DOUBLE) AS net_sales_amount,
            CAST('USD' AS STRING) AS currency_code
        FROM sales_transactions_silver sts
        LEFT JOIN product_silver ps
            ON sts.product_id = ps.product_id
        LEFT JOIN store_silver ss
            ON sts.store_id = ss.store_id
        """
    )

    (
        gold_sales_transactions_df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
    )

    # --------------------------------------------------------------------------------------
    # Target: gold_product
    # --------------------------------------------------------------------------------------
    gold_product_df = spark.sql(
        """
        SELECT
            CAST(ps.product_id AS STRING) AS product_id,
            CAST(ps.product_name AS STRING) AS product_name,
            CAST(ps.brand AS STRING) AS brand,
            CAST(ps.category AS STRING) AS category,
            CAST(ps.is_active AS BOOLEAN) AS is_active,
            CAST(NULL AS STRING) AS product_sku,
            CAST(NULL AS STRING) AS subcategory,
            CAST(NULL AS STRING) AS size,
            CAST(NULL AS STRING) AS unit_of_measure
        FROM product_silver ps
        """
    )

    (
        gold_product_df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/gold_product.csv")
    )

    # --------------------------------------------------------------------------------------
    # Target: gold_store
    # --------------------------------------------------------------------------------------
    gold_store_df = spark.sql(
        """
        SELECT
            CAST(ss.store_id AS STRING) AS store_id,
            CAST(ss.store_name AS STRING) AS store_name,
            CAST(ss.store_type AS STRING) AS store_type,
            CAST(ss.city AS STRING) AS city,
            CAST(ss.state AS STRING) AS state_province,
            CAST(ss.open_date AS DATE) AS open_date,
            CAST(NULL AS STRING) AS store_code,
            CAST(NULL AS STRING) AS region,
            CAST(NULL AS STRING) AS district,
            CAST(NULL AS STRING) AS country,
            CAST(NULL AS STRING) AS postal_code,
            CAST(NULL AS DATE) AS close_date,
            CAST(true AS BOOLEAN) AS is_active
        FROM store_silver ss
        """
    )

    (
        gold_store_df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/gold_store.csv")
    )

    # --------------------------------------------------------------------------------------
    # Target: gold_daily_sales_store_product
    # --------------------------------------------------------------------------------------
    gold_daily_sales_store_product_df = spark.sql(
        """
        SELECT
            DATE(CAST(sts.transaction_time AS TIMESTAMP)) AS sales_date,
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(SUM(CAST(sts.quantity AS INT)) AS INT) AS total_units_sold,
            CAST(SUM(CAST(sts.quantity AS DOUBLE) * CAST(ps.price AS DOUBLE)) AS DOUBLE) AS gross_sales_amount,
            CAST(0.0 AS DOUBLE) AS discount_amount,
            CAST(SUM(CAST(sts.quantity AS DOUBLE) * CAST(ps.price AS DOUBLE)) - 0.0 AS DOUBLE) AS net_sales_amount,
            CAST(COUNT(DISTINCT CAST(sts.transaction_id AS STRING)) AS BIGINT) AS transaction_count
        FROM sales_transactions_silver sts
        LEFT JOIN product_silver ps
            ON sts.product_id = ps.product_id
        GROUP BY
            DATE(CAST(sts.transaction_time AS TIMESTAMP)),
            CAST(sts.store_id AS STRING),
            CAST(sts.product_id AS STRING)
        """
    )

    (
        gold_daily_sales_store_product_df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/gold_daily_sales_store_product.csv")
    )

    # --------------------------------------------------------------------------------------
    # Target: gold_data_refresh_status
    # --------------------------------------------------------------------------------------
    # UDD expects one row per dataset load; we can populate available fields and default others.
    run_date = datetime.date.today().isoformat()

    sts_max_ts_df = spark.sql(
        """
        SELECT
            CAST(MAX(CAST(sts.transaction_time AS TIMESTAMP)) AS TIMESTAMP) AS source_max_extract_ts
        FROM sales_transactions_silver sts
        """
    )

    dataset_counts = [
        ("gold_sales_transactions", gold_sales_transactions_df.count()),
        ("gold_product", gold_product_df.count()),
        ("gold_store", gold_store_df.count()),
        ("gold_daily_sales_store_product", gold_daily_sales_store_product_df.count()),
    ]

    counts_df = spark.createDataFrame(dataset_counts, ["dataset_name", "records_processed"])

    gold_data_refresh_status_df = (
        counts_df.crossJoin(sts_max_ts_df)
        .withColumn("refresh_date", F.to_date(F.lit(run_date)))
        .withColumn("records_rejected", F.lit(0).cast("bigint"))
        .withColumn("refresh_start_ts", F.lit(None).cast("timestamp"))
        .withColumn("refresh_end_ts", F.lit(None).cast("timestamp"))
        .withColumn("refresh_status", F.lit(None).cast("string"))
        .select(
            F.col("dataset_name").cast("string"),
            F.col("refresh_date").cast("date"),
            F.col("refresh_start_ts").cast("timestamp"),
            F.col("refresh_end_ts").cast("timestamp"),
            F.col("refresh_status").cast("string"),
            F.col("source_max_extract_ts").cast("timestamp"),
            F.col("records_processed").cast("bigint"),
            F.col("records_rejected").cast("bigint"),
        )
    )

    (
        gold_data_refresh_status_df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/gold_data_refresh_status.csv")
    )

    job.commit()


if __name__ == "__main__":
    main()