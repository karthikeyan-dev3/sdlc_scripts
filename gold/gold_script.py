```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ----------------------------
# 1) Read source tables from S3
# ----------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# ----------------------------
# 2) Create temp views
# ----------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
products_silver_df.createOrReplaceTempView("products_silver")

# ============================================================
# TARGET TABLE: gold_sales_store_day
# ============================================================
gold_sales_store_day_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS date) AS sales_date,
        CAST(sts.store_id AS string) AS store_id,
        CAST(ss.store_name AS string) AS store_name,
        CAST(ss.city AS string) AS store_city,
        CAST(ss.state AS string) AS store_state,
        CAST(
            CASE
                WHEN ss.state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
                WHEN ss.state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
                WHEN ss.state IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
                WHEN ss.state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
                ELSE 'Other'
            END AS string
        ) AS store_region,
        CAST(SUM(CAST(sts.sale_amount AS double)) AS double) AS total_revenue,
        CAST(COUNT(DISTINCT CAST(sts.transaction_id AS string)) AS bigint) AS transaction_count,
        CAST(SUM(CAST(sts.quantity AS bigint)) AS bigint) AS quantity_sold
    FROM sales_transactions_silver sts
    LEFT JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    GROUP BY
        CAST(sts.sales_date AS date),
        CAST(sts.store_id AS string),
        CAST(ss.store_name AS string),
        CAST(ss.city AS string),
        CAST(ss.state AS string),
        CAST(
            CASE
                WHEN ss.state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
                WHEN ss.state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
                WHEN ss.state IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
                WHEN ss.state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
                ELSE 'Other'
            END AS string
        )
    """
)

(
    gold_sales_store_day_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_store_day.csv")
)

# ============================================================
# TARGET TABLE: gold_sales_product_day
# ============================================================
gold_sales_product_day_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS date) AS sales_date,
        CAST(sts.product_id AS string) AS product_id,
        CAST(ps.product_name AS string) AS product_name,
        CAST(ps.category AS string) AS category,
        CAST(ps.brand AS string) AS brand,
        CAST(SUM(CAST(sts.sale_amount AS double)) AS double) AS total_revenue,
        CAST(COUNT(DISTINCT CAST(sts.transaction_id AS string)) AS bigint) AS transaction_count,
        CAST(SUM(CAST(sts.quantity AS bigint)) AS bigint) AS quantity_sold
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        CAST(sts.sales_date AS date),
        CAST(sts.product_id AS string),
        CAST(ps.product_name AS string),
        CAST(ps.category AS string),
        CAST(ps.brand AS string)
    """
)

(
    gold_sales_product_day_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_product_day.csv")
)

# ============================================================
# TARGET TABLE: gold_sales_store_product_day
# ============================================================
gold_sales_store_product_day_df = spark.sql(
    """
    SELECT
        CAST(sts.sales_date AS date) AS sales_date,
        CAST(sts.store_id AS string) AS store_id,
        CAST(sts.product_id AS string) AS product_id,
        CAST(ps.category AS string) AS category,
        CAST(SUM(CAST(sts.sale_amount AS double)) AS double) AS total_revenue,
        CAST(COUNT(DISTINCT CAST(sts.transaction_id AS string)) AS bigint) AS transaction_count,
        CAST(SUM(CAST(sts.quantity AS bigint)) AS bigint) AS quantity_sold
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps
        ON sts.product_id = ps.product_id
    GROUP BY
        CAST(sts.sales_date AS date),
        CAST(sts.store_id AS string),
        CAST(sts.product_id AS string),
        CAST(ps.category AS string)
    """
)

(
    gold_sales_store_product_day_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_store_product_day.csv")
)

job.commit()
```