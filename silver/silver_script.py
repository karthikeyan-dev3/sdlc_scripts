```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, upper, trim, coalesce, sum as sql_sum
from pyspark.sql.window import Window

# Initialize Spark and Glue Contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Load and transform products_bronze
products_src_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
products_src_df.createOrReplaceTempView("products_bronze")

products_silver_df = spark.sql("""
    SELECT
        product_id,
        TRIM(UPPER(product_name)) AS product_name,
        TRIM(UPPER(category)) AS category,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_updated DESC) AS rn
    FROM products_bronze
    WHERE product_id IS NOT NULL
""").filter("rn = 1").drop("rn")

products_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/products_silver.csv", header=True)

# Load and transform stores_bronze
stores_src_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
stores_src_df.createOrReplaceTempView("stores_bronze")

stores_silver_df = spark.sql("""
    SELECT
        store_id,
        TRIM(UPPER(location)) AS location,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_updated DESC) AS rn
    FROM stores_bronze
    WHERE store_id IS NOT NULL
    """).filter("rn = 1").drop("rn")

stores_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/stores_silver.csv", header=True)

# Load and transform sales_transactions_bronze
sales_transactions_src_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
sales_transactions_src_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver_df = spark.sql("""
    SELECT
        stb.transaction_id,
        stb.store_id,
        stb.product_id,
        COALESCE(stb.quantity, 0) AS quantity,
        COALESCE(stb.sale_amount, 0) AS sale_amount,
        stb.transaction_time,
        CAST(stb.transaction_time AS date) AS sales_date,
        ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
    FROM sales_transactions_bronze stb
    INNER JOIN stores_silver ss ON stb.store_id = ss.store_id
    INNER JOIN products_silver ps ON stb.product_id = ps.product_id
    WHERE stb.transaction_id IS NOT NULL
""").filter("rn = 1").drop("rn")

sales_transactions_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)

# Aggregate daily sales
daily_sales_silver_df = spark.sql("""
    SELECT
        sts.store_id,
        sts.product_id,
        sts.sales_date AS date,
        SUM(sts.quantity) AS sales_quantity,
        SUM(sts.sale_amount) AS sales_revenue
    FROM sales_transactions_silver sts
    GROUP BY sts.store_id, sts.product_id, sts.sales_date
""")

daily_sales_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/daily_sales_silver.csv", header=True)

# Transform inventory levels
inventory_levels_silver_df = spark.sql("""
    SELECT
        dss.store_id,
        dss.product_id,
        dss.date,
        COALESCE(dss.sales_quantity, 0) AS inventory_quantity
    FROM daily_sales_silver dss
""")

inventory_levels_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/inventory_levels_silver.csv", header=True)

# Compute stockout status
stockout_status_silver_df = spark.sql("""
    SELECT
        ils.store_id,
        ils.product_id,
        ils.date,
        CASE WHEN ils.inventory_quantity <= 0 THEN 1 ELSE 0 END AS stockout_flag,
        CASE WHEN ils.inventory_quantity <= 0 THEN 'NEEDS_REPLENISHMENT' ELSE 'OK' END AS replenishment_status
    FROM inventory_levels_silver ils
""")

stockout_status_silver_df.write.mode('overwrite').csv(f"{TARGET_PATH}/stockout_status_silver.csv", header=True)
```