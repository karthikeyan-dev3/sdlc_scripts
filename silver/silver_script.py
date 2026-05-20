import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------
# 1) Read source tables (Bronze)
# --------------------------------------------------------------------
product_master_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)

store_master_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)

sales_performance_bronze_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_bronze.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")
sales_performance_bronze_df.createOrReplaceTempView("sales_performance_bronze")

# --------------------------------------------------------------------
# 3) Transform + Dedup: silver.product_master_silver
# --------------------------------------------------------------------
product_master_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(pmb.product_id))                           AS product_id,
    TRIM(pmb.product_name)                               AS product_name,
    TRIM(pmb.category)                                   AS category,
    TRIM(pmb.brand)                                      AS manufacturer,
    CAST(pmb.price AS DECIMAL(18,2))                     AS price,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(UPPER(pmb.product_id))
      ORDER BY
        (CASE WHEN TRIM(pmb.product_name) IS NOT NULL AND TRIM(pmb.product_name) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN TRIM(pmb.category) IS NOT NULL AND TRIM(pmb.category) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN TRIM(pmb.brand) IS NOT NULL AND TRIM(pmb.brand) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN pmb.price IS NOT NULL THEN 1 ELSE 0 END) DESC
    ) AS rn
  FROM product_master_bronze pmb
  WHERE COALESCE(LOWER(TRIM(pmb.is_active)), 'false') = 'true'
)
SELECT
  product_id,
  product_name,
  category,
  manufacturer,
  price
FROM base
WHERE rn = 1
"""

product_master_silver_df = spark.sql(product_master_silver_sql)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

# --------------------------------------------------------------------
# 4) Write: product_master_silver (single CSV directly under TARGET_PATH)
# --------------------------------------------------------------------
product_master_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/product_master_silver.csv"
)

# --------------------------------------------------------------------
# 3) Transform + Dedup: silver.store_master_silver
# --------------------------------------------------------------------
store_master_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(smb.store_id))                              AS store_id,
    TRIM(smb.store_name)                                  AS store_name,
    CONCAT(TRIM(smb.city), ', ', TRIM(smb.state))          AS location,
    CASE
      WHEN UPPER(TRIM(smb.state)) IN ('ME','NH','VT','MA','RI','CT','NY','NJ','PA') THEN 'NORTHEAST'
      WHEN UPPER(TRIM(smb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
      WHEN UPPER(TRIM(smb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
      WHEN UPPER(TRIM(smb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
      ELSE 'UNKNOWN'
    END                                                   AS region,
    TRIM(smb.store_type)                                  AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(UPPER(smb.store_id))
      ORDER BY
        (CASE WHEN TRIM(smb.store_name) IS NOT NULL AND TRIM(smb.store_name) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN TRIM(smb.city) IS NOT NULL AND TRIM(smb.city) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN TRIM(smb.state) IS NOT NULL AND TRIM(smb.state) <> '' THEN 1 ELSE 0 END) +
        (CASE WHEN TRIM(smb.store_type) IS NOT NULL AND TRIM(smb.store_type) <> '' THEN 1 ELSE 0 END) DESC
    ) AS rn
  FROM store_master_bronze smb
)
SELECT
  store_id,
  store_name,
  location,
  region,
  store_type
FROM base
WHERE rn = 1
"""

store_master_silver_df = spark.sql(store_master_silver_sql)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

# --------------------------------------------------------------------
# 4) Write: store_master_silver (single CSV directly under TARGET_PATH)
# --------------------------------------------------------------------
store_master_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/store_master_silver.csv"
)

# --------------------------------------------------------------------
# 3) Transform + Dedup + Conformance: silver.sales_performance_silver
# --------------------------------------------------------------------
sales_performance_silver_sql = """
WITH base AS (
  SELECT
    TRIM(UPPER(spb.transaction_id))                AS transaction_id,
    CAST(spb.transaction_time AS DATE)            AS sale_date,
    TRIM(UPPER(spb.product_id))                   AS product_id,
    TRIM(UPPER(spb.store_id))                     AS store_id,
    CAST(spb.quantity AS INT)                     AS quantity_sold,
    CAST(spb.sale_amount AS DECIMAL(18,2))        AS sales_amount,
    spb.transaction_time                          AS transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(UPPER(spb.transaction_id))
      ORDER BY spb.transaction_time DESC
    ) AS rn
  FROM sales_performance_bronze spb
),
dedup AS (
  SELECT
    transaction_id,
    sale_date,
    product_id,
    store_id,
    quantity_sold,
    sales_amount
  FROM base
  WHERE rn = 1
),
validated AS (
  SELECT
    transaction_id,
    sale_date,
    product_id,
    store_id,
    quantity_sold,
    sales_amount
  FROM dedup
  WHERE COALESCE(quantity_sold, 0) >= 0
    AND COALESCE(sales_amount, CAST(0 AS DECIMAL(18,2))) >= CAST(0 AS DECIMAL(18,2))
)
SELECT
  TRIM(UPPER(v.transaction_id)) AS transaction_id,
  v.sale_date                  AS sale_date,
  TRIM(UPPER(v.product_id))    AS product_id,
  TRIM(UPPER(v.store_id))      AS store_id,
  CAST(v.quantity_sold AS INT) AS quantity_sold,
  CAST(v.sales_amount AS DECIMAL(18,2)) AS sales_amount
FROM validated v
LEFT JOIN product_master_silver pms
  ON v.product_id = pms.product_id
LEFT JOIN store_master_silver sms
  ON v.store_id = sms.store_id
"""

sales_performance_silver_df = spark.sql(sales_performance_silver_sql)

# --------------------------------------------------------------------
# 4) Write: sales_performance_silver (single CSV directly under TARGET_PATH)
# --------------------------------------------------------------------
sales_performance_silver_df.coalesce(1).write.mode("overwrite").format("csv").option("header", "true").save(
    f"{TARGET_PATH}/sales_performance_silver.csv"
)

job.commit()
