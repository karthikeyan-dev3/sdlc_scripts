import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# 1) Read source tables (bronze) and create temp views
# -----------------------------------------------------------------------------------
pmb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
pmb_df.createOrReplaceTempView("product_master_bronze")

smb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)
smb_df.createOrReplaceTempView("store_master_bronze")

stb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stb_df.createOrReplaceTempView("sales_transactions_bronze")

# -----------------------------------------------------------------------------------
# 2) Target: silver.product_master_silver
# -----------------------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    SELECT
        pmb.product_id AS product_id,
        pmb.product_name AS product_name,
        pmb.category AS category,
        pmb.brand AS brand,
        CAST(pmb.price AS FLOAT) AS price
    FROM product_master_bronze pmb
    """
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# -----------------------------------------------------------------------------------
# 3) Target: silver.store_master_silver
# -----------------------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    SELECT
        smb.store_id AS store_id,
        smb.store_name AS store_name,
        smb.city AS location,
        smb.state AS region
    FROM store_master_bronze smb
    """
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# -----------------------------------------------------------------------------------
# 4) Target: silver.sales_transactions_silver
# -----------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    SELECT
        stb.transaction_id AS transaction_id,
        DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS sale_date,
        stb.product_id AS product_id,
        stb.store_id AS store_id,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount
    FROM sales_transactions_bronze stb
    INNER JOIN product_master_silver pms
        ON stb.product_id = pms.product_id
    INNER JOIN store_master_silver sms
        ON stb.store_id = sms.store_id
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# -----------------------------------------------------------------------------------
# 5) Target: silver.sales_aggregated_silver
# -----------------------------------------------------------------------------------
sales_aggregated_silver_df = spark.sql(
    """
    SELECT
        sts.sale_date AS sale_date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
        CAST(SUM(sts.total_sales_amount) AS DOUBLE) AS total_sales_amount
    FROM sales_transactions_silver sts
    GROUP BY
        sts.sale_date,
        sts.product_id,
        sts.store_id
    """
)

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()