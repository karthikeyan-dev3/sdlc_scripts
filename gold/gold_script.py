from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read and process gold_sales
sales_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
sales_df.createOrReplaceTempView("sts")

gold_sales_df = spark.sql("""
SELECT 
    transaction_id,
    sale_date,
    store_id,
    product_id,
    quantity_sold,
    sales_amount
FROM (
    SELECT 
        sts.transaction_id,
        CAST(sts.sale_date AS DATE) AS sale_date,
        sts.store_id,
        sts.product_id,
        COALESCE(sts.quantity_sold, 0) AS quantity_sold,
        COALESCE(sts.sales_amount, 0.0) AS sales_amount,
        ROW_NUMBER() OVER (PARTITION BY sts.transaction_id ORDER BY sts.transaction_id) as row_num
    FROM sts
) WHERE row_num = 1
""")
gold_sales_df.drop("row_num").write.mode("overwrite").format("csv").save(f"{TARGET_PATH}/gold_sales.csv")

# Read and process gold_product
product_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
product_df.createOrReplaceTempView("ps")

gold_product_df = spark.sql("""
SELECT 
    product_id,
    product_name,
    category,
    brand
FROM (
    SELECT 
        ps.product_id,
        TRIM(UPPER(ps.product_name)) AS product_name,
        TRIM(LOWER(ps.category)) AS category,
        TRIM(UPPER(ps.brand)) AS brand,
        ROW_NUMBER() OVER (PARTITION BY ps.product_id ORDER BY ps.product_id) as row_num
    FROM ps
) WHERE row_num = 1
""")
gold_product_df.drop("row_num").write.mode("overwrite").format("csv").save(f"{TARGET_PATH}/gold_product.csv")

# Read and process gold_store
store_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
store_df.createOrReplaceTempView("ss")

gold_store_df = spark.sql("""
SELECT 
    store_id,
    store_name,
    location,
    region
FROM (
    SELECT 
        ss.store_id,
        TRIM(ss.store_name) AS store_name,
        TRIM(ss.location) AS location,
        TRIM(ss.region) AS region,
        ROW_NUMBER() OVER (PARTITION BY ss.store_id ORDER BY ss.store_id) as row_num
    FROM ss
) WHERE row_num = 1
""")
gold_store_df.drop("row_num").write.mode("overwrite").format("csv").save(f"{TARGET_PATH}/gold_store.csv")

# Read and process gold_sales_aggregate
sales_aggregate_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_daily_aggregate_silver.{FILE_FORMAT}/")
sales_aggregate_df.createOrReplaceTempView("sdas")

gold_sales_aggregate_df = spark.sql("""
SELECT 
    store_id,
    product_id,
    total_quantity_sold,
    total_sales_amount,
    reporting_date
FROM (
    SELECT 
        sdas.store_id,
        sdas.product_id,
        COALESCE(sdas.total_quantity_sold, 0) AS total_quantity_sold,
        COALESCE(sdas.total_sales_amount, 0.0) AS total_sales_amount,
        CAST(sdas.reporting_date AS DATE) AS reporting_date,
        ROW_NUMBER() OVER (PARTITION BY sdas.store_id, sdas.product_id, sdas.reporting_date 
                           ORDER BY sdas.store_id, sdas.product_id, sdas.reporting_date) as row_num
    FROM sdas
) WHERE row_num = 1
""")
gold_sales_aggregate_df.drop("row_num").write.mode("overwrite").format("csv").save(f"{TARGET_PATH}/gold_sales_aggregate.csv")