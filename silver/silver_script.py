```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, row_number, trim, upper, lower, coalesce, cast, concat_ws
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("AWS Glue PySpark Script").getOrCreate()
glueContext = GlueContext(spark)

# Source paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read and process products_bronze
products_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
products_bronze_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql("""
    SELECT COALESCE(pb.product_id, '') AS product_id,
           COALESCE(UPPER(TRIM(pb.product_name)), '') AS product_name,
           COALESCE(LOWER(TRIM(pb.category)), '') AS product_category,
           ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY CURRENT_TIMESTAMP()) AS rn
    FROM products_bronze pb
    WHERE pb.price IS NOT NULL AND pb.price >= 0
""").filter(col("rn") == 1).drop("rn")

products_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/products_silver.csv", header=True)

# Read and process stores_bronze
stores_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

stores_silver_df = spark.sql("""
    SELECT COALESCE(sb.store_id, '') AS store_id,
           COALESCE(UPPER(TRIM(sb.store_name)), '') AS store_name,
           COALESCE(TRIM(sb.city), '') || ', ' || COALESCE(UPPER(TRIM(sb.state)), '') AS store_location,
           ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY CURRENT_TIMESTAMP()) AS rn
    FROM stores_bronze sb
""").filter(col("rn") == 1).drop("rn")

stores_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/stores_silver.csv", header=True)

# Read and process sales_transactions_bronze
sales_transactions_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql("""
    SELECT COALESCE(stb.transaction_id, '') AS transaction_id,
           COALESCE(stb.product_id, '') AS product_id,
           COALESCE(stb.store_id, '') AS store_id,
           CAST(stb.transaction_time AS DATE) AS sale_date,
           COALESCE(stb.quantity, 0) AS quantity_sold,
           COALESCE(stb.sale_amount, 0.0) AS total_sales_amount,
           ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
    FROM sales_transactions_bronze stb
    WHERE stb.quantity IS NOT NULL AND stb.quantity >= 0
      AND stb.sale_amount IS NOT NULL AND stb.sale_amount >= 0
      AND stb.transaction_time IS NOT NULL
""").filter(col("rn") == 1).drop("rn")

sales_transactions_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/sales_transactions_silver.csv", header=True)

# Enrich and process sales with products and stores
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

sales_enriched_silver_df = spark.sql("""
    SELECT sts.transaction_id,
           sts.product_id,
           sts.store_id,
           sts.sale_date,
           sts.quantity_sold,
           sts.total_sales_amount,
           ps.product_name,
           ps.product_category,
           ss.store_name,
           ss.store_location
    FROM sales_transactions_silver sts
    LEFT JOIN products_silver ps ON sts.product_id = ps.product_id
    LEFT JOIN stores_silver ss ON sts.store_id = ss.store_id
""")

sales_enriched_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/sales_enriched_silver.csv", header=True)

# Store sales daily aggregation
sales_enriched_silver_df.createOrReplaceTempView("sales_enriched_silver")

store_sales_daily_silver_df = spark.sql("""
    SELECT ses.store_id,
           ses.store_name,
           ses.store_location,
           ses.sale_date AS date,
           SUM(ses.total_sales_amount) AS total_sales,
           COUNT(DISTINCT ses.transaction_id) AS number_of_transactions
    FROM sales_enriched_silver ses
    GROUP BY ses.store_id, ses.store_name, ses.store_location, ses.sale_date
""")

store_sales_daily_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/store_sales_daily_silver.csv", header=True)

# Product sales daily aggregation
product_sales_daily_silver_df = spark.sql("""
    SELECT ses.product_id,
           ses.product_name,
           ses.product_category,
           ses.sale_date AS date,
           SUM(ses.total_sales_amount) AS total_sales,
           SUM(ses.quantity_sold) AS quantity_sold
    FROM sales_enriched_silver ses
    GROUP BY ses.product_id, ses.product_name, ses.product_category, ses.sale_date
""")

product_sales_daily_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/product_sales_daily_silver.csv", header=True)

# Sales daily summary aggregation
sales_daily_summary_silver_df = spark.sql("""
    SELECT sts.sale_date AS date,
           SUM(sts.total_sales_amount) AS total_sales,
           SUM(sts.quantity_sold) AS total_quantity_sold,
           COUNT(DISTINCT sts.transaction_id) AS number_of_transactions
    FROM sales_transactions_silver sts
    GROUP BY sts.sale_date
""")

sales_daily_summary_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/sales_daily_summary_silver.csv", header=True)

# Data quality metrics
data_quality_metrics_silver_df = spark.sql("""
    SELECT sts.sale_date AS date,
           COUNT(sts.transaction_id) AS total_records_processed,
           (COUNT(sts.transaction_id) - COUNT(DISTINCT sts.transaction_id)) AS duplicates_removed
    FROM sales_transactions_silver sts
    GROUP BY sts.sale_date
""")

data_quality_metrics_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/data_quality_metrics_silver.csv", header=True)
```