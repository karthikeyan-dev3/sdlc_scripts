```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContex

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

product_attributes_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_attributes_silver.{FILE_FORMAT}/")
)

store_attributes_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_attributes_silver.{FILE_FORMAT}/")
)

data_quality_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
product_attributes_silver_df.createOrReplaceTempView("product_attributes_silver")
store_attributes_silver_df.createOrReplaceTempView("store_attributes_silver")
data_quality_silver_df.createOrReplaceTempView("data_quality_silver")

# ------------------------------------------------------------
# Target: gold_sales_transactions (gst)
# Mapping: silver.sales_transactions_silver sts
# ------------------------------------------------------------
gold_sales_transactions_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sts.transaction_id AS STRING) AS transaction_id,
            CAST(sts.transaction_date AS DATE) AS transaction_date,
            CAST(sts.product_id AS STRING) AS product_id,
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(sts.quantity_sold AS INT) AS quantity_sold,
            CAST(sts.sales_amount AS DOUBLE) AS sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(sts.transaction_id AS STRING)
                ORDER BY CAST(sts.transaction_id AS STRING)
            ) AS rn
        FROM sales_transactions_silver sts
    )
    SELECT
        transaction_id,
        transaction_date,
        product_id,
        store_id,
        quantity_sold,
        sales_amount
    FROM base
    WHERE rn = 1
    """
)

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ------------------------------------------------------------
# Target: gold_product_attributes (gpa)
# Mapping: silver.product_attributes_silver pas
# ------------------------------------------------------------
gold_product_attributes_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pas.product_id AS STRING) AS product_id,
            CAST(pas.product_name AS STRING) AS product_name,
            CAST(pas.category AS STRING) AS category,
            CAST(pas.price AS FLOAT) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(pas.product_id AS STRING)
                ORDER BY CAST(pas.product_id AS STRING)
            ) AS rn
        FROM product_attributes_silver pas
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM base
    WHERE rn = 1
    """
)

(
    gold_product_attributes_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_attributes.csv")
)

# ------------------------------------------------------------
# Target: gold_store_attributes (gsa)
# Mapping: silver.store_attributes_silver sas
# ------------------------------------------------------------
gold_store_attributes_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sas.store_id AS STRING) AS store_id,
            CAST(sas.store_name AS STRING) AS store_name,
            CAST(sas.location AS STRING) AS location,
            CAST(sas.store_size AS STRING) AS store_size,
            CAST(sas.region AS STRING) AS region,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(sas.store_id AS STRING)
                ORDER BY CAST(sas.store_id AS STRING)
            ) AS rn
        FROM store_attributes_silver sas
    )
    SELECT
        store_id,
        store_name,
        location,
        store_size,
        region
    FROM base
    WHERE rn = 1
    """
)

(
    gold_store_attributes_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_attributes.csv")
)

# ------------------------------------------------------------
# Target: gold_sales_report (gsr)
# Mapping: silver.sales_transactions_silver sts
#          LEFT JOIN silver.product_attributes_silver pas ON sts.product_id = pas.product_id
# ------------------------------------------------------------
gold_sales_report_df = spark.sql(
    """
    WITH sales_by_product AS (
        SELECT
            CAST(sts.transaction_date AS DATE) AS report_date,
            CAST(sts.store_id AS STRING) AS store_id,
            CAST(sts.product_id AS STRING) AS product_id,
            SUM(CAST(sts.sales_amount AS DOUBLE)) AS product_sales,
            SUM(CAST(sts.quantity_sold AS BIGINT)) AS product_units
        FROM sales_transactions_silver sts
        LEFT JOIN product_attributes_silver pas
            ON CAST(sts.product_id AS STRING) = CAST(pas.product_id AS STRING)
        GROUP BY
            CAST(sts.transaction_date AS DATE),
            CAST(sts.store_id AS STRING),
            CAST(sts.product_id AS STRING)
    ),
    ranked_products AS (
        SELECT
            report_date,
            store_id,
            product_id AS top_selling_product_id,
            ROW_NUMBER() OVER (
                PARTITION BY report_date, store_id
                ORDER BY product_sales DESC, product_units DESC, product_id
            ) AS rn
        FROM sales_by_product
    ),
    sales_by_store_date AS (
        SELECT
            CAST(sts.transaction_date AS DATE) AS report_date,
            CAST(sts.store_id AS STRING) AS store_id,
            SUM(CAST(sts.sales_amount AS DOUBLE)) AS total_sales,
            SUM(CAST(sts.quantity_sold AS BIGINT)) AS total_units_sold,
            (SUM(CAST(sts.sales_amount AS DOUBLE)) / SUM(CAST(sts.quantity_sold AS DOUBLE))) AS average_sales_price
        FROM sales_transactions_silver sts
        GROUP BY
            CAST(sts.transaction_date AS DATE),
            CAST(sts.store_id AS STRING)
    )
    SELECT
        sbsd.report_date,
        sbsd.store_id,
        sbsd.total_sales,
        sbsd.total_units_sold,
        sbsd.average_sales_price,
        rp.top_selling_product_id
    FROM sales_by_store_date sbsd
    LEFT JOIN ranked_products rp
        ON sbsd.report_date = rp.report_date
       AND sbsd.store_id = rp.store_id
       AND rp.rn = 1
    """
)

(
    gold_sales_report_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_report.csv")
)

# ------------------------------------------------------------
# Target: gold_data_quality (gdq)
# Mapping: silver.data_quality_silver dqs
# ------------------------------------------------------------
gold_data_quality_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(dqs.validation_date AS DATE) AS validation_date,
            CAST(dqs.dataset_name AS STRING) AS dataset_name,
            CAST(dqs.total_records AS BIGINT) AS total_records,
            CAST(dqs.invalid_records AS BIGINT) AS invalid_records,
            CAST(dqs.duplicate_records AS BIGINT) AS duplicate_records,
            CAST(dqs.data_quality_score AS DOUBLE) AS data_quality_score,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(dqs.validation_date AS DATE), CAST(dqs.dataset_name AS STRING)
                ORDER BY CAST(dqs.validation_date AS DATE), CAST(dqs.dataset_name AS STRING)
            ) AS rn
        FROM data_quality_silver dqs
    )
    SELECT
        validation_date,
        dataset_name,
        total_records,
        invalid_records,
        duplicate_records,
        data_quality_score
    FROM base
    WHERE rn = 1
    """
)

(
    gold_data_quality_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality.csv")
)

job.commit()
```