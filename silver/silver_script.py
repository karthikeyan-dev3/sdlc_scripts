
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -----------------------------------------------------------------------------------
# Glue bootstrap
# -----------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ===================================================================================
# TABLE: categories_silver
# Source: bronze.categories_bronze cb
# ===================================================================================

# 1) Read source tables from S3
categories_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/categories_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
categories_bronze_df.createOrReplaceTempView("categories_bronze")

# 3) SQL transformations (incl. ROW_NUMBER for dedup)
categories_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(cb.category AS STRING) AS category_raw,
    CAST(cb.category_standard AS STRING) AS category,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(cb.category AS STRING)
      ORDER BY CAST(cb.category_standard AS STRING) DESC
    ) AS rn
  FROM categories_bronze cb
)
SELECT
  category_raw,
  category
FROM base
WHERE rn = 1
""")

categories_silver_df.createOrReplaceTempView("categories_silver")

# 4) Save output as SINGLE CSV under TARGET_PATH (no subfolders in path string)
(
    categories_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/categories_silver.csv")
)

# ===================================================================================
# TABLE: stores_silver
# Source: bronze.stores_bronze sb
# ===================================================================================

# 1) Read source tables from S3
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# 3) SQL transformations (incl. ROW_NUMBER for dedup)
stores_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(sb.store_id AS STRING) AS store_id,
    CAST(sb.store_name AS STRING) AS store_name,
    CAST(sb.store_type AS STRING) AS store_type,
    CAST(sb.city AS STRING) AS city,
    CAST(sb.state AS STRING) AS state,
    CAST(sb.country AS STRING) AS country,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(sb.store_id AS STRING)
      ORDER BY CAST(sb.store_id AS STRING) ASC
    ) AS rn
  FROM stores_bronze sb
)
SELECT
  store_id,
  store_name,
  store_type,
  city,
  state,
  country
FROM base
WHERE rn = 1
""")

# 4) Save output as SINGLE CSV under TARGET_PATH (no subfolders in path string)
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ===================================================================================
# TABLE: products_silver
# Source: bronze.products_bronze pb LEFT JOIN silver.categories_silver cs ON pb.category = cs.category_raw
# ===================================================================================

# 1) Read source tables from S3
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "false")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

# 2) Create temp views
products_bronze_df.createOrReplaceTempView("products_bronze")
# NOTE: categories_silver temp view already created above

# 3) SQL transformations (incl. ROW_NUMBER for dedup)
products_silver_df = spark.sql("""
WITH joined AS (
  SELECT
    CAST(pb.product_id AS STRING) AS product_id,
    CAST(pb.product_name AS STRING) AS product_name,
    CAST(pb.brand AS STRING) AS brand,
    CAST(cs.category AS STRING) AS category,
    ROW_NUMBER() OVER (
      PARTITION BY CAST(pb.product_id AS STRING)
      ORDER BY CAST(pb.product_id AS STRING) ASC
    ) AS rn
  FROM products_bronze pb
  LEFT JOIN categories_silver cs
    ON CAST(pb.category AS STRING) = CAST(cs.category_raw AS STRING)
)
SELECT
  product_id,
  product_name,
  brand,
  category
FROM joined
WHERE rn = 1
""")

# 4) Save output as SINGLE CSV under TARGET_PATH (no subfolders in path string)
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

job.commit()
