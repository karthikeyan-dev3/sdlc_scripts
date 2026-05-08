from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, trim, upper, row_number, coalesce
from pyspark.sql.window import Window

# Initialize Spark and Glue Context
spark = SparkSession.builder \
    .appName("AWS Glue PySpark Job") \
    .getOrCreate()
glueContext = GlueContext(spark)

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"

# Read and process products_bronze
products_bronze = spark.read.format('csv').option("header", "true").load(f"{SOURCE_PATH}/products_bronze.csv/")
products_bronze.createOrReplaceTempView("pb")

products_silver_query = """
SELECT 
    CAST(pb.product_id AS STRING) AS product_id,
    UPPER(TRIM(pb.product_name)) AS product_name,
    UPPER(TRIM(pb.category)) AS category,
    UPPER(TRIM(pb.brand)) AS brand,
    CAST(pb.price AS DOUBLE) AS price,
    CASE WHEN pb.is_active = 'true' THEN 'AVAILABLE' ELSE 'NOT_AVAILABLE' END AS availability_status
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) AS row_num
    FROM pb
    WHERE product_id IS NOT NULL
) 
WHERE row_num = 1
"""

products_silver_df = spark.sql(products_silver_query)
products_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/products_silver.csv", header=True)

# Read and process stores_bronze
stores_bronze = spark.read.format('csv').option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.csv/")
stores_bronze.createOrReplaceTempView("sb")

stores_silver_query = """
SELECT 
    CAST(sb.store_id AS STRING) AS store_id,
    UPPER(TRIM(sb.store_name)) AS store_name,
    CONCAT(TRIM(sb.city), ',', TRIM(sb.state)) AS location,
    UPPER(TRIM(sb.store_type)) AS store_type,
    CAST(sb.open_date AS DATE) AS opening_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY updated_at DESC) AS row_num
    FROM sb
    WHERE store_id IS NOT NULL
)
WHERE row_num = 1
"""

stores_silver_df = spark.sql(stores_silver_query)
stores_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/stores_silver.csv", header=True)

# Read and process sales_transactions_bronze
sales_transactions_bronze = spark.read.format('csv').option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.csv/")
sales_transactions_bronze.createOrReplaceTempView("stb")

# Ensure products_silver and stores_silver are registered for joins
products_silver_df.createOrReplaceTempView("ps")
stores_silver_df.createOrReplaceTempView("ss")

sales_transactions_silver_query = """
SELECT
    stb.transaction_id,
    stb.product_id,
    stb.store_id,
    DATE(stb.transaction_time) AS transaction_date,
    stb.quantity AS quantity_sold,
    stb.sale_amount AS total_sales_amount,
    COALESCE(stb.sales_channel, 'IN_STORE') AS sales_channel
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY updated_at DESC) AS row_num
    FROM stb
    WHERE quantity >= 0 AND sale_amount >= 0
)
WHERE row_num = 1
AND product_id IN (SELECT product_id FROM ps)
AND store_id IN (SELECT store_id FROM ss)
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_query)
sales_transactions_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/sales_transactions_silver.csv", header=True)

# Read and process sales_aggregates_silver
sales_aggregates_silver_query = """
SELECT
    sts.transaction_date AS date,
    sts.store_id,
    sts.product_id,
    ps.category,
    SUM(sts.quantity_sold) AS total_quantity_sold,
    SUM(sts.total_sales_amount) AS total_sales_amount
FROM sales_transactions_silver sts
INNER JOIN products_silver ps ON sts.product_id = ps.product_id
GROUP BY
    sts.transaction_date,
    sts.store_id,
    sts.product_id,
    ps.category
"""

sales_aggregates_silver_df = spark.sql(sales_aggregates_silver_query)
sales_aggregates_silver_df.write.mode("overwrite").csv(TARGET_PATH + "/sales_aggregates_silver.csv", header=True)