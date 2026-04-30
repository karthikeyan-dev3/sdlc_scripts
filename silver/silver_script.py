```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, coalesce, trim, upper, lower, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Glue PySpark Job") \
    .getOrCreate()

glueContext = GlueContext(spark)

# Read bronze POS sales event table
pos_sales_event_df = spark.read.format("csv").option("header", "true").load(
    f"s3://sdlc-agent-bucket/engineering-agent/bronze/pos_sales_event_bronze.csv/")

# Read bronze payment gateway event table
payment_gateway_event_df = spark.read.format("csv").option("header", "true").load(
    f"s3://sdlc-agent-bucket/engineering-agent/bronze/payment_gateway_event_bronze.csv/")

# Create temp views for SQL transformations
pos_sales_event_df.createOrReplaceTempView("pseb")
payment_gateway_event_df.createOrReplaceTempView("pgeb_pre")

# Process payments deduplication
sql_dedup_payment = """
SELECT
    payment_id,
    transaction_id,
    payment_mode,
    provider,
    amount,
    currency,
    payment_status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY 
               CASE WHEN payment_status IN ('SUCCESS','CAPTURED','SETTLED') THEN 1 ELSE 2 END,
               amount DESC) AS rn
    FROM pgeb_pre
)
WHERE rn = 1
"""
dedup_payment_df = spark.sql(sql_dedup_payment)
dedup_payment_df.createOrReplaceTempView("pgeb")

# Sales Transactions Silver
sql_sales_transactions = """
SELECT DISTINCT
    pseb.transaction_id,
    pseb.event_action,
    pseb.store_id,
    pseb.product_id,
    pseb.quantity AS quantity_sold,
    pseb.total_amount AS total_sales_amount,
    CAST(DATE(pseb.timestamp) AS DATE) AS sale_date
FROM pseb
LEFT JOIN pgeb
ON pseb.payment_id = pgeb.payment_id
WHERE pseb.transaction_id IS NOT NULL
  AND pseb.store_id IS NOT NULL
  AND pseb.product_id IS NOT NULL
  AND pseb.quantity > 0
  AND pseb.total_amount > 0
  AND (pgeb.payment_status IN ('SUCCESS', 'CAPTURED', 'SETTLED') OR pgeb.payment_id IS NULL)
"""
sales_transactions_df = spark.sql(sql_sales_transactions)
sales_transactions_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv", header=True)

# Product Master Silver
sql_product_master = """
SELECT
    pseb.product_id,
    UPPER(TRIM(pseb.product_name)) AS product_name,
    LOWER(TRIM(pseb.category)) AS category,
    COALESCE(NULL, 'UNKNOWN') AS brand
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY timestamp DESC) AS rn FROM pseb) pseb
WHERE pseb.product_id IS NOT NULL
  AND rn = 1
"""
product_master_df = spark.sql(sql_product_master)
product_master_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/product_master_silver.csv", header=True)

# Store Master Silver
sql_store_master = """
SELECT
    pseb.store_id,
    CONCAT('STORE_', pseb.store_id) AS store_name,
    COALESCE(NULL, 'UNKNOWN') AS location
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY timestamp DESC) AS rn FROM pseb) pseb
WHERE pseb.store_id IS NOT NULL
  AND rn = 1
"""
store_master_df = spark.sql(sql_store_master)
store_master_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/store_master_silver.csv", header=True)

# Daily Store Product Sales Agg Silver
sales_transactions_df.createOrReplaceTempView("sts")

sql_daily_store_product_sales = """
SELECT
    sts.sale_date AS aggregation_date,
    sts.store_id,
    sts.product_id,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    SUM(sts.total_sales_amount) AS total_sales_amount
FROM sts
GROUP BY sts.sale_date, sts.store_id, sts.product_id
"""
daily_store_product_sales_df = spark.sql(sql_daily_store_product_sales)
daily_store_product_sales_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/daily_store_product_sales_agg_silver.csv", header=True)
```