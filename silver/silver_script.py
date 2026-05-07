from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, trim, upper, coalesce, sum as spark_sum, avg, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, DoubleType

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Process product_silver table
products_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_bronze.csv/")
products_df.createOrReplaceTempView("products_bronze")

product_silver_df = spark.sql("""
    SELECT 
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        UPPER(TRIM(pb.product_id)) AS sku
    FROM products_bronze pb
    WHERE pb.product_name IS NOT NULL
    DISTRIBUTE BY product_id
""")
product_silver_df.write.csv(f"{TARGET_PATH}/product_silver.csv", header=True, mode="overwrite")

# Process sales_daily_silver table
sales_transactions_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.csv/")
stores_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.csv/")
sales_transactions_df.createOrReplaceTempView("sales_transactions_bronze")
stores_df.createOrReplaceTempView("stores_bronze")
product_silver_df.createOrReplaceTempView("product_silver")

sales_daily_silver_df = spark.sql("""
    SELECT 
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        CAST(stb.transaction_time AS DATE) AS date,
        SUM(COALESCE(stb.quantity, 0)) AS sales_qty
    FROM sales_transactions_bronze stb
    JOIN stores_bronze sb ON TRIM(stb.store_id) = TRIM(sb.store_id)
    JOIN product_silver ps ON TRIM(stb.product_id) = ps.product_id
    WHERE stb.store_id IS NOT NULL AND stb.product_id IS NOT NULL AND COALESCE(stb.quantity, 0) >= 0
    GROUP BY TRIM(stb.store_id), TRIM(stb.product_id), CAST(stb.transaction_time AS DATE)
""")
sales_daily_silver_df.write.csv(f"{TARGET_PATH}/sales_daily_silver.csv", header=True, mode="overwrite")

# Process store_product_day_spine_silver table
sales_daily_silver_df.createOrReplaceTempView("sales_daily_silver")

store_product_day_spine_silver_df = spark.sql("""
    SELECT DISTINCT
        sds.store_id,
        sds.product_id,
        sds.date
    FROM sales_daily_silver sds
""")
store_product_day_spine_silver_df.write.csv(f"{TARGET_PATH}/store_product_day_spine_silver.csv", header=True, mode="overwrite")

# Process sales_inventory_daily_silver table
store_product_day_spine_silver_df.createOrReplaceTempView("store_product_day_spine_silver")

sales_inventory_daily_silver_df = spark.sql("""
    SELECT 
        spds.store_id,
        spds.product_id,
        spds.date,
        COALESCE(sds.sales_qty, 0) AS sales_qty,
        CAST(NULL AS INT) AS inventory_qty,
        CASE WHEN COALESCE(sds.sales_qty, 0) = 0 THEN 1 ELSE 0 END AS stockout_flag
    FROM store_product_day_spine_silver spds
    LEFT JOIN sales_daily_silver sds ON spds.store_id = sds.store_id AND spds.product_id = sds.product_id AND spds.date = sds.date
""")
sales_inventory_daily_silver_df.write.csv(f"{TARGET_PATH}/sales_inventory_daily_silver.csv", header=True, mode="overwrite")

# Process kpi_daily_product_silver table
sales_inventory_daily_silver_df.createOrReplaceTempView("sales_inventory_daily_silver")

kpi_daily_product_silver_df = spark.sql("""
    SELECT 
        sids.date,
        sids.product_id,
        CAST(NULL AS DOUBLE) AS stockout_rate,
        AVG(COALESCE(sids.sales_qty, 0)) OVER (
            PARTITION BY sids.product_id 
            ORDER BY sids.date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS sales_velocity,
        CAST(NULL AS DOUBLE) AS revenue_leakage_index
    FROM sales_inventory_daily_silver sids
""")
kpi_daily_product_silver_df.write.csv(f"{TARGET_PATH}/kpi_daily_product_silver.csv", header=True, mode="overwrite")