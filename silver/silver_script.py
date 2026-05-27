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

# --------------------------------------------------------------------------------------
# Read source tables (bronze) from S3
# --------------------------------------------------------------------------------------
sales_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
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

# --------------------------------------------------------------------------------------
# Create temp views
# --------------------------------------------------------------------------------------
sales_bronze_df.createOrReplaceTempView("sales_bronze")
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")

# --------------------------------------------------------------------------------------
# Target: silver.product_master_silver
# --------------------------------------------------------------------------------------
product_master_silver_sql = """
WITH base AS (
  SELECT
    pmb.product_id AS product_id,
    TRIM(pmb.product_name) AS product_name,
    UPPER(TRIM(pmb.category)) AS category,
    UPPER(TRIM(pmb.brand)) AS brand,
    CAST(pmb.price AS float) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY pmb.product_id
      ORDER BY pmb.product_id
    ) AS rn
  FROM product_master_bronze pmb
  WHERE pmb.is_active = true
)
SELECT
  product_id,
  product_name,
  category,
  brand,
  price
FROM base
WHERE rn = 1
"""

product_master_silver_df = spark.sql(product_master_silver_sql)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.store_master_silver
# --------------------------------------------------------------------------------------
store_master_silver_sql = """
WITH base AS (
  SELECT
    smb.store_id AS store_id,
    TRIM(smb.store_name) AS store_name,
    CONCAT(smb.city, ', ', smb.state) AS location,
    smb.state AS region,
    smb.store_type AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY smb.store_id
      ORDER BY smb.store_id
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

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.sales_silver
# --------------------------------------------------------------------------------------
sales_silver_sql = """
WITH joined AS (
  SELECT
    sb.transaction_id AS sale_id,
    sb.product_id AS product_id,
    sb.store_id AS store_id,
    CAST(sb.transaction_time AS date) AS sale_date,
    sb.quantity AS quantity_sold,
    sb.sale_amount AS sales_amount,
    sb.transaction_time AS transaction_time,
    ROW_NUMBER() OVER (
      PARTITION BY sb.transaction_id
      ORDER BY sb.transaction_time DESC
    ) AS rn
  FROM sales_bronze sb
  INNER JOIN product_master_bronze pmb
    ON sb.product_id = pmb.product_id
  INNER JOIN store_master_bronze smb
    ON sb.store_id = smb.store_id
  WHERE pmb.is_active = true
)
SELECT
  sale_id,
  product_id,
  store_id,
  sale_date,
  quantity_sold,
  sales_amount
FROM joined
WHERE rn = 1
"""

sales_silver_df = spark.sql(sales_silver_sql)
sales_silver_df.createOrReplaceTempView("sales_silver")

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.sales_aggregate_silver
# Note: MAP_AGG is not available in Spark SQL; using map_from_entries + collect_list of structs.
# --------------------------------------------------------------------------------------
sales_aggregate_silver_sql = """
WITH base AS (
  SELECT
    ss.sale_date AS sale_date,
    ss.sales_amount AS sales_amount,
    ss.quantity_sold AS quantity_sold,
    sms.region AS region,
    pms.category AS category
  FROM sales_silver ss
  INNER JOIN product_master_silver pms
    ON ss.product_id = pms.product_id
  INNER JOIN store_master_silver sms
    ON ss.store_id = sms.store_id
),
by_region AS (
  SELECT
    sale_date,
    region,
    SUM(sales_amount) AS region_sales_amount
  FROM base
  GROUP BY sale_date, region
),
by_category AS (
  SELECT
    sale_date,
    category,
    SUM(sales_amount) AS category_sales_amount
  FROM base
  GROUP BY sale_date, category
),
region_map AS (
  SELECT
    sale_date,
    map_from_entries(collect_list(named_struct('key', region, 'value', region_sales_amount))) AS total_sales_by_region
  FROM by_region
  GROUP BY sale_date
),
category_map AS (
  SELECT
    sale_date,
    map_from_entries(collect_list(named_struct('key', category, 'value', category_sales_amount))) AS total_sales_by_product_category
  FROM by_category
  GROUP BY sale_date
),
daily AS (
  SELECT
    sale_date,
    SUM(sales_amount) AS total_sales_amount,
    SUM(quantity_sold) AS total_quantity_sold,
    AVG(sales_amount) AS average_sale_amount
  FROM base
  GROUP BY sale_date
)
SELECT
  d.sale_date AS sale_date,
  d.total_sales_amount AS total_sales_amount,
  d.total_quantity_sold AS total_quantity_sold,
  d.average_sale_amount AS average_sale_amount,
  rm.total_sales_by_region AS total_sales_by_region,
  cm.total_sales_by_product_category AS total_sales_by_product_category
FROM daily d
LEFT JOIN region_map rm
  ON d.sale_date = rm.sale_date
LEFT JOIN category_map cm
  ON d.sale_date = cm.sale_date
"""

sales_aggregate_silver_df = spark.sql(sales_aggregate_silver_sql)
sales_aggregate_silver_df.createOrReplaceTempView("sales_aggregate_silver")

(
    sales_aggregate_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregate_silver.csv")
)

# --------------------------------------------------------------------------------------
# Target: silver.data_quality_metrics_silver
# --------------------------------------------------------------------------------------
data_quality_metrics_silver_sql = """
WITH processed AS (
  SELECT
    CURRENT_DATE AS execution_date,
    COUNT(ss.sale_id) AS num_records_processed
  FROM sales_silver ss
),
dupes AS (
  SELECT
    (COUNT(sb.transaction_id) - COUNT(DISTINCT sb.transaction_id)) AS duplicate_records_count,
    1 - (
      (COUNT(sb.transaction_id) - COUNT(DISTINCT sb.transaction_id))
      / NULLIF(COUNT(sb.transaction_id), 0)
    ) AS data_quality_score
  FROM sales_bronze sb
)
SELECT
  p.execution_date AS execution_date,
  p.num_records_processed AS num_records_processed,
  d.duplicate_records_count AS duplicate_records_count,
  d.data_quality_score AS data_quality_score
FROM processed p
CROSS JOIN dupes d
"""

data_quality_metrics_silver_df = spark.sql(data_quality_metrics_silver_sql)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()
