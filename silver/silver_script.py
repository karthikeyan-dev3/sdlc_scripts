```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

spark = SparkSession.builder.appName("GlueJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# Load source tables from S3
pos_sales_event_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/pos_sales_event_bronze.{FILE_FORMAT}/")
payment_gateway_payment_event_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/payment_gateway_payment_event_bronze.{FILE_FORMAT}/")

# Create temp views
pos_sales_event_df.createOrReplaceTempView("pseb")
payment_gateway_payment_event_df.createOrReplaceTempView("pgb")

# sales_transactions_silver
sales_transactions_query = """
SELECT
    pseb.transaction_id AS sale_id,
    pseb.transaction_id,
    pseb.store_id,
    pseb.product_id,
    pseb.quantity AS quantity_sold,
    pseb.total_amount AS revenue,
    pseb.payment_id,
    pg.payment_mode,
    pg.provider,
    pg.currency,
    pg.payment_status
FROM pseb
LEFT JOIN (
    SELECT
        payment_id,
        transaction_id,
        payment_mode,
        provider,
        amount,
        currency,
        payment_status,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY payment_status DESC) AS rn
    FROM pgb
) pg
ON pseb.payment_id = pg.payment_id AND pg.rn = 1
"""

sales_transactions_df = spark.sql(sales_transactions_query)
output_sales_transactions_path = f"{TARGET_PATH}/sales_transactions_silver.csv"
sales_transactions_df.coalesce(1).write.csv(output_sales_transactions_path, mode="overwrite", header=True)

# product_master_silver
product_master_query = """
SELECT 
    pseb.product_id,
    MAX(NULLIF(TRIM(pseb.product_name), '')) AS product_name,
    MAX(NULLIF(TRIM(pseb.category), '')) AS category,
    CAST(NULL AS STRING) AS brand
FROM pseb
GROUP BY pseb.product_id
"""

product_master_df = spark.sql(product_master_query)
output_product_master_path = f"{TARGET_PATH}/product_master_silver.csv"
product_master_df.coalesce(1).write.csv(output_product_master_path, mode="overwrite", header=True)

# store_master_silver
store_master_query = """
SELECT DISTINCT
    store_id,
    CAST(NULL AS STRING) AS store_name,
    CAST(NULL AS STRING) AS store_type,
    CAST(NULL AS STRING) AS city,
    CAST(NULL AS STRING) AS region
FROM pseb
"""

store_master_df = spark.sql(store_master_query)
output_store_master_path = f"{TARGET_PATH}/store_master_silver.csv"
store_master_df.coalesce(1).write.csv(output_store_master_path, mode="overwrite", header=True)

# sales_daily_aggregation_silver
sales_transactions_df.createOrReplaceTempView("st")
store_master_df.createOrReplaceTempView("sm")

sales_daily_aggregation_query = """
SELECT 
    CAST(st.transaction_date AS DATE) AS reporting_date,
    SUM(st.revenue) AS total_revenue,
    SUM(st.quantity_sold) AS total_quantity_sold
FROM st 
LEFT JOIN sm ON st.store_id = sm.store_id
GROUP BY CAST(st.transaction_date AS DATE)
"""

sales_daily_aggregation_df = spark.sql(sales_daily_aggregation_query)
output_sales_daily_aggregation_path = f"{TARGET_PATH}/sales_daily_aggregation_silver.csv"
sales_daily_aggregation_df.coalesce(1).write.csv(output_sales_daily_aggregation_path, mode="overwrite", header=True)
```