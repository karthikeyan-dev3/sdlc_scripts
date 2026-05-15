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

# ------------------------------------------------------------
# Read Source Tables (Bronze)
# ------------------------------------------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------
# TARGET: silver.products_silver
# ------------------------------------------------------------
products_silver_sql = """
WITH base AS (
  SELECT
    pb.product_id AS product_id,
    TRIM(UPPER(REGEXP_REPLACE(pb.product_name, '\\\\s+', ' '))) AS standardized_product_name,
    TRIM(pb.category) AS product_category
  FROM products_bronze pb
),
dedup AS (
  SELECT
    product_id,
    standardized_product_name,
    product_category,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        CASE WHEN standardized_product_name IS NOT NULL AND standardized_product_name <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN product_category IS NOT NULL AND product_category <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  product_id,
  standardized_product_name,
  product_category
FROM dedup
WHERE rn = 1
"""
products_silver_df = spark.sql(products_silver_sql)
products_silver_df.createOrReplaceTempView("products_silver")

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------
# TARGET: silver.stores_silver
# ------------------------------------------------------------
stores_silver_sql = """
WITH base AS (
  SELECT
    sb.store_id AS store_id,
    TRIM(UPPER(REGEXP_REPLACE(sb.store_name, '\\\\s+', ' '))) AS standardized_store_name,
    CASE
      WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NE'
      WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MW'
      WHEN UPPER(TRIM(sb.state)) IN ('DE','DC','FL','GA','MD','NC','SC','VA','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'S'
      WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'W'
    END AS store_region
  FROM stores_bronze sb
),
dedup AS (
  SELECT
    store_id,
    standardized_store_name,
    store_region,
    ROW_NUMBER() OVER (
      PARTITION BY store_id
      ORDER BY
        CASE WHEN standardized_store_name IS NOT NULL AND standardized_store_name <> '' THEN 1 ELSE 0 END DESC,
        CASE WHEN store_region IS NOT NULL AND store_region <> '' THEN 1 ELSE 0 END DESC
    ) AS rn
  FROM base
)
SELECT
  store_id,
  standardized_store_name,
  store_region
FROM dedup
WHERE rn = 1
"""
stores_silver_df = spark.sql(stores_silver_sql)
stores_silver_df.createOrReplaceTempView("stores_silver")

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------
# TARGET: silver.sales_transactions_silver
# ------------------------------------------------------------
sales_transactions_silver_sql = """
WITH joined AS (
  SELECT
    stb.transaction_id AS transaction_id,
    CAST(stb.transaction_time AS date) AS sale_date,
    stb.product_id AS product_id,
    stb.store_id AS store_id,
    CAST(stb.quantity AS int) AS quantity_sold,
    CAST(stb.sale_amount AS double) AS total_sales_amount,
    ps.standardized_product_name AS standardized_product_name,
    ss.standardized_store_name AS standardized_store_name,
    ps.product_category AS product_category,
    ss.store_region AS store_region,
    stb.transaction_time AS transaction_time
  FROM sales_transactions_bronze stb
  INNER JOIN products_silver ps
    ON stb.product_id = ps.product_id
  INNER JOIN stores_silver ss
    ON stb.store_id = ss.store_id
),
validated AS (
  SELECT
    transaction_id,
    sale_date,
    product_id,
    store_id,
    CASE WHEN quantity_sold > 0 THEN quantity_sold END AS quantity_sold,
    CASE WHEN total_sales_amount >= 0 THEN total_sales_amount END AS total_sales_amount,
    standardized_product_name,
    standardized_store_name,
    product_category,
    store_region,
    transaction_time
  FROM joined
),
dedup AS (
  SELECT
    transaction_id,
    sale_date,
    product_id,
    store_id,
    quantity_sold,
    total_sales_amount,
    standardized_product_name,
    standardized_store_name,
    product_category,
    store_region,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM validated
)
SELECT
  transaction_id,
  sale_date,
  product_id,
  store_id,
  quantity_sold,
  total_sales_amount,
  standardized_product_name,
  standardized_store_name,
  product_category,
  store_region
FROM dedup
WHERE rn = 1
"""
sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ------------------------------------------------------------
# TARGET: silver.daily_sales_aggregation_silver
# ------------------------------------------------------------
daily_sales_aggregation_silver_sql = """
WITH per_day_product AS (
  SELECT
    sts.sale_date AS sale_date,
    sts.product_id AS product_id,
    SUM(sts.total_sales_amount) AS product_revenue
  FROM sales_transactions_silver sts
  GROUP BY sts.sale_date, sts.product_id
),
ranked_product AS (
  SELECT
    sale_date,
    product_id,
    ROW_NUMBER() OVER (
      PARTITION BY sale_date
      ORDER BY product_revenue DESC, product_id
    ) AS rn
  FROM per_day_product
),
top_product AS (
  SELECT
    sale_date,
    product_id AS top_selling_product
  FROM ranked_product
  WHERE rn = 1
),
per_day_store AS (
  SELECT
    sts.sale_date AS sale_date,
    sts.store_id AS store_id,
    SUM(sts.total_sales_amount) AS store_revenue
  FROM sales_transactions_silver sts
  GROUP BY sts.sale_date, sts.store_id
),
ranked_store AS (
  SELECT
    sale_date,
    store_id,
    ROW_NUMBER() OVER (
      PARTITION BY sale_date
      ORDER BY store_revenue DESC, store_id
    ) AS rn
  FROM per_day_store
),
top_store AS (
  SELECT
    sale_date,
    store_id AS top_selling_store
  FROM ranked_store
  WHERE rn = 1
),
daily AS (
  SELECT
    sts.sale_date AS aggregated_date,
    SUM(sts.quantity_sold) AS total_sales_volume,
    SUM(sts.total_sales_amount) AS total_revenue,
    AVG(sts.total_sales_amount) AS average_sales_value
  FROM sales_transactions_silver sts
  GROUP BY sts.sale_date
)
SELECT
  d.aggregated_date,
  d.total_sales_volume,
  d.total_revenue,
  d.average_sales_value,
  tp.top_selling_product,
  ts.top_selling_store
FROM daily d
LEFT JOIN top_product tp
  ON d.aggregated_date = tp.sale_date
LEFT JOIN top_store ts
  ON d.aggregated_date = ts.sale_date
"""
daily_sales_aggregation_silver_df = spark.sql(daily_sales_aggregation_silver_sql)
daily_sales_aggregation_silver_df.createOrReplaceTempView("daily_sales_aggregation_silver")

(
    daily_sales_aggregation_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/daily_sales_aggregation_silver.csv")
)

job.commit()
