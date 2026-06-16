import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ----------------------------
# 1) Read source tables (Bronze)
# ----------------------------
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

# ----------------------------
# 2) product_master_silver
# ----------------------------
product_master_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        pmb.product_id AS product_id,
        pmb.product_name AS product_name,
        pmb.category AS product_category,
        ROW_NUMBER() OVER (
          PARTITION BY pmb.product_id
          ORDER BY pmb.product_id DESC
        ) AS rn
      FROM product_master_bronze pmb
    )
    SELECT
      product_id,
      product_name,
      product_category
    FROM ranked
    WHERE rn = 1
    """
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver")
)

# ----------------------------
# 3) store_master_silver
# ----------------------------
store_master_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        smb.store_id AS store_id,
        smb.store_name AS store_name,
        smb.state AS store_region,
        ROW_NUMBER() OVER (
          PARTITION BY smb.store_id
          ORDER BY smb.store_id DESC
        ) AS rn
      FROM store_master_bronze smb
    )
    SELECT
      store_id,
      store_name,
      store_region
    FROM ranked
    WHERE rn = 1
    """
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver")
)

# ----------------------------
# 4) sales_transactions_silver
# ----------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        stb.transaction_id AS transaction_id,
        stb.store_id AS store_id,
        stb.product_id AS product_id,
        CAST(stb.transaction_time AS DATE) AS sale_date,
        CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
        CAST(stb.quantity AS INT) AS quantity_sold,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY stb.transaction_time DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      LEFT JOIN product_master_silver pms
        ON stb.product_id = pms.product_id
      LEFT JOIN store_master_silver sms
        ON stb.store_id = sms.store_id
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      sale_date,
      sale_amount,
      quantity_sold
    FROM ranked
    WHERE rn = 1
      AND sale_amount >= 0
      AND quantity_sold >= 0
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver")
)

# ----------------------------
# 5) sales_aggregated_silver
# ----------------------------
sales_aggregated_silver_df = spark.sql(
    """
    SELECT
      CAST(sts.sale_date AS DATE) AS aggregation_date,
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      SUM(sts.sale_amount) AS total_sales_amount,
      SUM(sts.quantity_sold) AS total_quantity_sold
    FROM sales_transactions_silver sts
    GROUP BY
      CAST(sts.sale_date AS DATE),
      sts.store_id,
      sts.product_id
    """
)

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver")
)

job.commit()