import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =============================================================================
# 1) Read source tables (Bronze)
# =============================================================================
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

product_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)

store_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)

# =============================================================================
# 2) Create temp views
# =============================================================================
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")

# =============================================================================
# 3) Transform + Write: silver.sales_transactions_silver
# =============================================================================
sales_transactions_silver_sql = """
WITH base AS (
  SELECT
    stb.transaction_id AS transaction_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    stb.store_id AS store_id,
    stb.product_id AS product_id,
    CAST(stb.quantity AS INT) AS quantity,
    CAST(stb.sale_amount AS DOUBLE) AS total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY stb.transaction_id
      ORDER BY stb.transaction_time DESC
    ) AS rn
  FROM sales_transactions_bronze stb
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity,
    total_amount
  FROM base
  WHERE rn = 1
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  quantity,
  total_amount
FROM dedup
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# =============================================================================
# 4) Transform + Write: silver.product_master_silver
# =============================================================================
product_master_silver_sql = """
WITH base AS (
  SELECT
    pmb.product_id AS product_id,
    TRIM(pmb.product_name) AS product_name,
    TRIM(pmb.category) AS category,
    CAST(pmb.price AS FLOAT) AS price,
    TRIM(pmb.brand) AS brand,
    ROW_NUMBER() OVER (
      PARTITION BY pmb.product_id
      ORDER BY pmb.product_id
    ) AS rn
  FROM product_master_bronze pmb
),
dedup AS (
  SELECT
    product_id,
    product_name,
    category,
    price,
    brand
  FROM base
  WHERE rn = 1
)
SELECT
  product_id,
  product_name,
  category,
  price,
  brand
FROM dedup
"""

product_master_silver_df = spark.sql(product_master_silver_sql)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# =============================================================================
# 5) Transform + Write: silver.store_master_silver
# =============================================================================
store_master_silver_sql = """
WITH base AS (
  SELECT
    smb.store_id AS store_id,
    TRIM(smb.store_name) AS store_name,
    CONCAT(TRIM(smb.city), ', ', TRIM(smb.state)) AS location,
    TRIM(smb.store_type) AS store_type,
    CAST(smb.open_date AS DATE) AS opening_date,
    ROW_NUMBER() OVER (
      PARTITION BY smb.store_id
      ORDER BY smb.store_id
    ) AS rn
  FROM store_master_bronze smb
),
dedup AS (
  SELECT
    store_id,
    store_name,
    location,
    store_type,
    opening_date
  FROM base
  WHERE rn = 1
)
SELECT
  store_id,
  store_name,
  location,
  store_type,
  opening_date
FROM dedup
"""

store_master_silver_df = spark.sql(store_master_silver_sql)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# =============================================================================
# 6) Transform + Write: silver.daily_sales_summary_silver
# =============================================================================
daily_sales_summary_silver_sql = """
WITH daily_agg AS (
  SELECT
    sts.transaction_date AS date,
    SUM(sts.total_amount) AS total_sales_amount,
    SUM(sts.quantity) AS total_units_sold,
    AVG(sts.total_amount) AS average_transaction_value
  FROM sales_transactions_silver sts
  GROUP BY sts.transaction_date
),
top_product AS (
  SELECT
    x.date,
    x.product_id AS top_selling_product_id
  FROM (
    SELECT
      sts.transaction_date AS date,
      sts.product_id AS product_id,
      SUM(sts.quantity) AS qty_sum,
      SUM(sts.total_amount) AS amt_sum,
      ROW_NUMBER() OVER (
        PARTITION BY sts.transaction_date
        ORDER BY SUM(sts.quantity) DESC, SUM(sts.total_amount) DESC, sts.product_id ASC
      ) AS rn
    FROM sales_transactions_silver sts
    GROUP BY sts.transaction_date, sts.product_id
  ) x
  WHERE x.rn = 1
),
top_store AS (
  SELECT
    y.date,
    y.store_id AS top_selling_store_id
  FROM (
    SELECT
      sts.transaction_date AS date,
      sts.store_id AS store_id,
      SUM(sts.total_amount) AS amt_sum,
      SUM(sts.quantity) AS qty_sum,
      ROW_NUMBER() OVER (
        PARTITION BY sts.transaction_date
        ORDER BY SUM(sts.total_amount) DESC, SUM(sts.quantity) DESC, sts.store_id ASC
      ) AS rn
    FROM sales_transactions_silver sts
    GROUP BY sts.transaction_date, sts.store_id
  ) y
  WHERE y.rn = 1
)
SELECT
  a.date AS date,
  a.total_sales_amount AS total_sales_amount,
  a.total_units_sold AS total_units_sold,
  a.average_transaction_value AS average_transaction_value,
  p.top_selling_product_id AS top_selling_product_id,
  s.top_selling_store_id AS top_selling_store_id
FROM daily_agg a
LEFT JOIN top_product p
  ON a.date = p.date
LEFT JOIN top_store s
  ON a.date = s.date
"""

daily_sales_summary_silver_df = spark.sql(daily_sales_summary_silver_sql)
daily_sales_summary_silver_df.createOrReplaceTempView("daily_sales_summary_silver")

(
    daily_sales_summary_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_summary_silver.csv")
)

job.commit()
