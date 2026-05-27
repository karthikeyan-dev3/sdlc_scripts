import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Source: silver.sales_silver (ss)
# Target: gold.gold_sales
# -------------------------------------------------------------------
sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)
sales_silver_df.createOrReplaceTempView("sales_silver")

gold_sales_df = spark.sql(
    """
    SELECT
        ss.sale_id AS sale_id,
        ss.product_id AS product_id,
        ss.store_id AS store_id,
        ss.sale_date AS sale_date,
        ss.quantity_sold AS quantity_sold,
        ss.sales_amount AS sales_amount
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

# -------------------------------------------------------------------
# Source: silver.product_master_silver (pms)
# Target: gold.gold_product_master
# -------------------------------------------------------------------
product_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

gold_product_master_df = spark.sql(
    """
    SELECT
        pms.product_id AS product_id,
        TRIM(pms.product_name) AS product_name,
        UPPER(TRIM(pms.category)) AS category,
        UPPER(TRIM(pms.brand)) AS brand,
        CAST(pms.price AS float) AS price
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

# -------------------------------------------------------------------
# Source: silver.store_master_silver (sms)
# Target: gold.gold_store_master
# -------------------------------------------------------------------
store_master_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

gold_store_master_df = spark.sql(
    """
    SELECT
        sms.store_id AS store_id,
        TRIM(sms.store_name) AS store_name,
        CONCAT(sms.city, ', ', sms.state) AS location,
        sms.region AS region,
        sms.store_type AS store_type
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

# -------------------------------------------------------------------
# Source: silver.sales_aggregate_silver (sas)
# Target: gold.gold_sales_aggregate
# -------------------------------------------------------------------
sales_aggregate_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregate_silver.{FILE_FORMAT}/")
)
sales_aggregate_silver_df.createOrReplaceTempView("sales_aggregate_silver")

gold_sales_aggregate_df = spark.sql(
    """
    SELECT
        sas.sale_date AS sale_date,
        sas.total_sales_amount AS total_sales_amount,
        sas.total_quantity_sold AS total_quantity_sold,
        sas.average_sale_amount AS average_sale_amount,
        sas.total_sales_by_region AS total_sales_by_region,
        sas.total_sales_by_product_category AS total_sales_by_product_category
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

# -------------------------------------------------------------------
# Source: silver.data_quality_metrics_silver (dqms)
# Target: gold.gold_data_quality_metrics
# -------------------------------------------------------------------
data_quality_metrics_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

gold_data_quality_metrics_df = spark.sql(
    """
    SELECT
        dqms.execution_date AS execution_date,
        dqms.num_records_processed AS num_records_processed,
        dqms.duplicate_records_count AS duplicate_records_count,
        dqms.data_quality_score AS data_quality_score
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

job.commit()
