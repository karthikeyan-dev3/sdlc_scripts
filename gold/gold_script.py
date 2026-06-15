```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -----------------------------------------------------------------------------------
# 1) Read source tables from S3
# -----------------------------------------------------------------------------------
dim_store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_store_silver.{FILE_FORMAT}/")
)

dim_product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_product_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

# -----------------------------------------------------------------------------------
# 2) Create temp views
# -----------------------------------------------------------------------------------
dim_store_silver_df.createOrReplaceTempView("dim_store_silver")
dim_product_silver_df.createOrReplaceTempView("dim_product_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -----------------------------------------------------------------------------------
# Target Table: gold_dim_store
# -----------------------------------------------------------------------------------
gold_dim_store_df = spark.sql(
    """
    SELECT
        dss.store_id      AS store_id,
        dss.store_name    AS store_name,
        dss.city          AS store_city,
        dss.state         AS store_state,
        dss.store_region  AS store_region,
        dss.store_type    AS store_type,
        dss.open_date     AS open_date,
        dss.active_flag   AS active_flag
    FROM dim_store_silver dss
    """
)

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

# -----------------------------------------------------------------------------------
# Target Table: gold_dim_product
# -----------------------------------------------------------------------------------
gold_dim_product_df = spark.sql(
    """
    SELECT
        dps.product_id     AS product_id,
        dps.product_name   AS product_name,
        dps.brand          AS brand,
        dps.category       AS category,
        dps.subcategory    AS subcategory,
        dps.unit_of_measure AS unit_of_measure,
        dps.active_flag    AS active_flag
    FROM dim_product_silver dps
    """
)

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

# -----------------------------------------------------------------------------------
# Target Table: gold_sales_daily_store
# -----------------------------------------------------------------------------------
gold_sales_daily_store_df = spark.sql(
    """
    SELECT
        sts.sales_date                              AS sales_date,
        sts.store_id                                AS store_id,
        dss.store_name                              AS store_name,
        dss.city                                    AS store_city,
        dss.state                                   AS store_state,
        dss.store_region                            AS store_region,
        dss.store_type                              AS store_type,
        COUNT(DISTINCT sts.transaction_id)          AS transactions_count,
        SUM(sts.quantity)                           AS quantity_sold,
        SUM(sts.sale_amount)                        AS gross_revenue,
        SUM(sts.sale_amount)                        AS net_revenue
    FROM sales_transactions_silver sts
    INNER JOIN dim_store_silver dss
        ON sts.store_id = dss.store_id
    GROUP BY
        sts.sales_date,
        sts.store_id,
        dss.store_name,
        dss.city,
        dss.state,
        dss.store_region,
        dss.store_type
    """
)

(
    gold_sales_daily_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_store.csv")
)

# -----------------------------------------------------------------------------------
# Target Table: gold_sales_daily_product
# -----------------------------------------------------------------------------------
gold_sales_daily_product_df = spark.sql(
    """
    SELECT
        sts.sales_date                              AS sales_date,
        sts.product_id                              AS product_id,
        dps.product_name                            AS product_name,
        dps.brand                                   AS brand,
        dps.category                                AS category,
        dps.subcategory                             AS subcategory,
        COUNT(DISTINCT sts.transaction_id)          AS transactions_count,
        SUM(sts.quantity)                           AS quantity_sold,
        SUM(sts.sale_amount)                        AS gross_revenue,
        SUM(sts.sale_amount)                        AS net_revenue
    FROM sales_transactions_silver sts
    INNER JOIN dim_product_silver dps
        ON sts.product_id = dps.product_id
    GROUP BY
        sts.sales_date,
        sts.product_id,
        dps.product_name,
        dps.brand,
        dps.category,
        dps.subcategory
    """
)

(
    gold_sales_daily_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_product.csv")
)

# -----------------------------------------------------------------------------------
# Target Table: gold_sales_daily_store_product
# -----------------------------------------------------------------------------------
gold_sales_daily_store_product_df = spark.sql(
    """
    SELECT
        sts.sales_date                              AS sales_date,
        sts.store_id                                AS store_id,
        sts.product_id                              AS product_id,
        COUNT(DISTINCT sts.transaction_id)          AS transactions_count,
        SUM(sts.quantity)                           AS quantity_sold,
        SUM(sts.sale_amount)                        AS gross_revenue,
        SUM(sts.sale_amount)                        AS net_revenue
    FROM sales_transactions_silver sts
    GROUP BY
        sts.sales_date,
        sts.store_id,
        sts.product_id
    """
)

(
    gold_sales_daily_store_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_daily_store_product.csv")
)

job.commit()
```