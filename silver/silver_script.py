from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, coalesce, concat, current_timestamp, current_date, row_number, sum as sum_, avg, percent_rank
from pyspark.sql.window import Window

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = SparkSession.builder.getOrCreate()

# Set paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read and process products_bronze
products_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
products_bronze_df.createOrReplaceTempView("products_bronze")

window_spec_product = Window.partitionBy("product_id").orderBy("product_id")
products_silver_df = products_bronze_df.withColumn("row_num", row_number().over(window_spec_product)) \
    .filter(col("row_num") == 1) \
    .select(trim(col("product_id")).alias("product_id"), trim(col("category")).alias("category"))
products_silver_df.createOrReplaceTempView("products_silver")

products_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/products_silver.csv", header=True)

# Read and process stores_bronze
stores_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

window_spec_store = Window.partitionBy("store_id").orderBy("store_id")
stores_silver_df = stores_bronze_df.withColumn("row_num", row_number().over(window_spec_store)) \
    .filter(col("row_num") == 1) \
    .select(trim(col("store_id")).alias("store_id"), \
            concat(trim(col("city")), lit(", "), trim(col("state"))).alias("location"))
stores_silver_df.createOrReplaceTempView("stores_silver")

stores_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/stores_silver.csv", header=True)

# Read and process sales_transactions_bronze
sales_transactions_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql("""
SELECT
  TRIM(stb.transaction_id) AS transaction_id,
  ss.store_id,
  ps.product_id,
  CAST(stb.transaction_time AS DATE) AS sale_date,
  stb.quantity AS quantity_sold,
  stb.sale_amount AS total_revenue,
  COALESCE(TRIM(sb.store_type), 'UNKNOWN') AS sales_channel
FROM
  (SELECT *, ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) as row_num FROM sales_transactions_bronze stb) stb
LEFT JOIN stores_silver ss ON TRIM(stb.store_id) = ss.store_id
LEFT JOIN products_silver ps ON TRIM(stb.product_id) = ps.product_id
WHERE stb.row_num = 1
""")

sales_transactions_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)

# Read and process sales_transactions_silver for store_daily_sales_silver
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

store_daily_sales_silver_df = spark.sql("""
SELECT
  sts.store_id,
  sts.sale_date,
  SUM(sts.total_revenue) AS daily_revenue,
  COUNT(DISTINCT sts.transaction_id) AS daily_transactions
FROM
  sales_transactions_silver sts
GROUP BY
  sts.store_id, sts.sale_date
""")

store_daily_sales_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/store_daily_sales_silver.csv", header=True)

# Read and process store_performance_silver
store_daily_sales_silver_df.createOrReplaceTempView("store_daily_sales_silver")

store_performance_silver_df = spark.sql("""
SELECT
  sdss.store_id,
  ss.location,
  SUM(sdss.daily_revenue) AS total_revenue,
  AVG(sdss.daily_revenue) AS average_daily_sales,
  CURRENT_TIMESTAMP AS last_updated
FROM
  store_daily_sales_silver sdss
INNER JOIN stores_silver ss ON sdss.store_id = ss.store_id
GROUP BY
  sdss.store_id, ss.location
""")

store_performance_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/store_performance_silver.csv", header=True)

# Read and process product_daily_sales_silver
product_daily_sales_silver_df = spark.sql("""
SELECT
  sts.product_id,
  sts.sale_date,
  SUM(sts.quantity_sold) AS daily_units_sold,
  SUM(sts.total_revenue) AS daily_revenue
FROM
  sales_transactions_silver sts
GROUP BY
  sts.product_id, sts.sale_date
""")

product_daily_sales_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/product_daily_sales_silver.csv", header=True)

# Read and process product_performance_silver
product_daily_sales_silver_df.createOrReplaceTempView("product_daily_sales_silver")
products_silver_df.createOrReplaceTempView("products_silver")

product_performance_silver_df = spark.sql("""
SELECT
  pdss.product_id,
  ps.category,
  SUM(pdss.daily_units_sold) AS total_units_sold,
  SUM(pdss.daily_revenue) AS total_revenue,
  (PERCENT_RANK() OVER (PARTITION BY ps.category ORDER BY SUM(pdss.daily_revenue) DESC) <= 0.10) AS top_products_flag
FROM
  product_daily_sales_silver pdss
INNER JOIN products_silver ps ON pdss.product_id = ps.product_id
GROUP BY
  pdss.product_id, ps.category
""")

product_performance_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/product_performance_silver.csv", header=True)

# Read and process data_quality_silver
data_quality_silver_df = spark.sql("""
SELECT
  stb.transaction_id AS record_id,
  (stb.transaction_id IS NOT NULL AND stb.store_id IS NOT NULL AND stb.product_id IS NOT NULL AND stb.transaction_time IS NOT NULL AND stb.quantity >= 0 AND stb.sale_amount >= 0 AND sb.store_id IS NOT NULL AND pb.product_id IS NOT NULL) AS data_validity_flag,
  (COUNT(*) OVER (PARTITION BY stb.transaction_id) > 1) AS duplicates_flag,
  CURRENT_TIMESTAMP AS last_validated
FROM
  sales_transactions_bronze stb
LEFT JOIN stores_bronze sb ON stb.store_id = sb.store_id
LEFT JOIN products_bronze pb ON stb.product_id = pb.product_id
""")

data_quality_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/data_quality_silver.csv", header=True)

# Read and process dashboard_refresh_silver
dashboard_refresh_silver_df = spark.sql("""
SELECT
  CURRENT_DATE AS last_refresh_date,
  CASE WHEN COUNT(sps.store_id) > 0 AND COUNT(pps.product_id) > 0 THEN 'SUCCESS' ELSE 'FAILED' END AS refresh_status
FROM
  store_performance_silver sps
CROSS JOIN product_performance_silver pps
CROSS JOIN (SELECT 1 AS x) ctl
""")

dashbard_refresh_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/dashboard_refresh_silver.csv", header=True)
