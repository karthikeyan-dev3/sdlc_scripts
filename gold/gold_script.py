import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------
# Read Source Tables (S3 -> Spark)
# -------------------------------

sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)
sales_silver_df.createOrReplaceTempView("sales_silver")

product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

sales_aggregate_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregate_silver.{FILE_FORMAT}/")
)
sales_aggregate_silver_df.createOrReplaceTempView("sales_aggregate_silver")

data_quality_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

# -------------------------------
# Target: gold_sales
# -------------------------------

gold_sales_df = spark.sql(
    """
    SELECT
        CAST(ss.sale_id AS STRING) AS sale_id,
        CAST(ss.product_id AS STRING) AS product_id,
        CAST(ss.store_id AS STRING) AS store_id,
        CAST(ss.sale_date AS DATE) AS sale_date,
        CAST(ss.quantity_sold AS INT) AS quantity_sold,
        CAST(ss.sales_amount AS DOUBLE) AS sales_amount
    FROM sales_silver ss
    """
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# -------------------------------
# Target: gold_product_master
# -------------------------------

gold_product_master_df = spark.sql(
    """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        CAST(pms.product_name AS STRING) AS product_name,
        CAST(pms.category AS STRING) AS category,
        CAST(pms.brand AS STRING) AS brand,
        CAST(pms.price AS FLOAT) AS price
    FROM product_master_silver pms
    """
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# -------------------------------
# Target: gold_store_master
# -------------------------------

gold_store_master_df = spark.sql(
    """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        CAST(sms.store_name AS STRING) AS store_name,
        CAST(sms.location AS STRING) AS location,
        CAST(sms.region AS STRING) AS region,
        CAST(sms.store_type AS STRING) AS store_type
    FROM store_master_silver sms
    """
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# -------------------------------
# Target: gold_sales_aggregate
# -------------------------------

gold_sales_aggregate_df = spark.sql(
    """
    SELECT
        CAST(sas.sale_date AS DATE) AS sale_date,
        CAST(sas.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(sas.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sas.average_sale_amount AS DOUBLE) AS average_sale_amount,
        CAST(sas.total_sales_by_region AS STRING) AS total_sales_by_region,
        CAST(sas.total_sales_by_product_category AS STRING) AS total_sales_by_product_category
    FROM sales_aggregate_silver sas
    """
)

(
    gold_sales_aggregate_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregate.csv")
)

# -------------------------------
# Target: gold_data_quality_metrics
# -------------------------------

gold_data_quality_metrics_df = spark.sql(
    """
    SELECT
        CAST(dqms.execution_date AS DATE) AS execution_date,
        CAST(dqms.num_records_processed AS BIGINT) AS num_records_processed
    FROM data_quality_metrics_silver dqms
    """
)

(
    gold_data_quality_metrics_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_data_quality_metrics.csv")
)
