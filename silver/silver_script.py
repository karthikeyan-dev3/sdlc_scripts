import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()  # Initialize SparkContext

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# 1) Read source tables (Bronze) and create temp views
# -------------------------------------------------------------------
product_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_bronze.{FILE_FORMAT}/")
)
product_bronze_df.createOrReplaceTempView("product_bronze")

store_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_bronze.{FILE_FORMAT}/")
)
store_bronze_df.createOrReplaceTempView("store_bronze")

sales_transaction_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transaction_bronze.{FILE_FORMAT}/")
)
sales_transaction_bronze_df.createOrReplaceTempView("sales_transaction_bronze")

# -------------------------------------------------------------------
# 2) product_silver
# -------------------------------------------------------------------
product_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(pb.product_id AS STRING) AS product_id,
        CAST(pb.product_name AS STRING) AS product_name,
        CAST(pb.category AS STRING) AS category,
        CAST(pb.brand AS STRING) AS brand,
        CAST(pb.is_active AS BOOLEAN) AS is_active,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id
        ) AS rn
      FROM product_bronze pb
      WHERE pb.product_id IS NOT NULL
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      is_active
    FROM ranked
    WHERE rn = 1
    """
)
product_silver_df.createOrReplaceTempView("product_silver")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# -------------------------------------------------------------------
# 3) store_silver
# -------------------------------------------------------------------
store_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(sb.store_id AS STRING) AS store_id,
        CAST(sb.store_name AS STRING) AS store_name,
        CAST(sb.city AS STRING) AS city,
        CAST(sb.state AS STRING) AS state,
        CAST(sb.store_type AS STRING) AS store_type,
        DATE(CAST(sb.open_date AS STRING)) AS open_date,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id
        ) AS rn
      FROM store_bronze sb
      WHERE sb.store_id IS NOT NULL
    )
    SELECT
      store_id,
      store_name,
      city,
      state,
      store_type,
      open_date
    FROM ranked
    WHERE rn = 1
    """
)
store_silver_df.createOrReplaceTempView("store_silver")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# -------------------------------------------------------------------
# 4) sales_transaction_silver
# -------------------------------------------------------------------
sales_transaction_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        CAST(stb.transaction_id AS STRING) AS transaction_id,
        CAST(stb.store_id AS STRING) AS store_id,
        CAST(stb.product_id AS STRING) AS product_id,
        CAST(stb.quantity AS INT) AS quantity,
        CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        ROW_NUMBER() OVER (
          PARTITION BY
            stb.transaction_id,
            stb.store_id,
            stb.product_id,
            stb.transaction_time
          ORDER BY stb.transaction_id
        ) AS rn
      FROM sales_transaction_bronze stb
      INNER JOIN store_silver ss
        ON stb.store_id = ss.store_id
      INNER JOIN product_silver ps
        ON stb.product_id = ps.product_id
      WHERE
        stb.transaction_id IS NOT NULL
        AND stb.store_id IS NOT NULL
        AND stb.product_id IS NOT NULL
    )
    SELECT
      transaction_id,
      store_id,
      product_id,
      quantity,
      sale_amount,
      transaction_time
    FROM ranked
    WHERE rn = 1
    """
)

(
    sales_transaction_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transaction_silver.csv")
)

job.commit()