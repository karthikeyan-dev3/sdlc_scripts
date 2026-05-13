
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, row_number, trim, upper, coalesce, lit
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

JOB_NAME = "GlueTransformationJob"
job = Job(glueContext)
job.init(JOB_NAME, {})

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------- Transformations for products_silver ---------

# Read products_bronze
products_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")

# Create temp view
products_bronze_df.createOrReplaceTempView("products_bronze")

# Transform SQL
products_silver_query = """
SELECT
    TRIM(UPPER(pb.product_id)) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    COALESCE(pb.is_active, true) AS is_active
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) as rn
    FROM products_bronze
    WHERE COALESCE(is_active, true) = true
) pb
WHERE pb.rn = 1
"""

# Execute SQL and save
products_silver_df = spark.sql(products_silver_query)
products_silver_df.write \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/products_silver.csv")

# --------- Transformations for stores_silver ---------

# Read stores_bronze
stores_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")

# Create temp view
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# Transform SQL
stores_silver_query = """
SELECT
    TRIM(UPPER(sb.store_id)) AS store_id,
    TRIM(sb.store_name) AS store_name,
    TRIM(sb.city) AS city,
    TRIM(sb.state) AS state
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY updated_at DESC) as rn
    FROM stores_bronze
) sb
WHERE sb.rn = 1
"""

# Execute SQL and save
stores_silver_df = spark.sql(stores_silver_query)
stores_silver_df.write \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/stores_silver.csv")

# --------- Transformations for sales_transactions_silver ---------

# Read sales_transactions_bronze
sales_transactions_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")

# Create temp view
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# Transform SQL
sales_transactions_silver_query = """
SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    CAST(TRIM(UPPER(stb.store_id)) AS varchar(10)) AS store_id,
    CAST(TRIM(UPPER(stb.product_id)) AS varchar(10)) AS product_id,
    CAST(stb.transaction_time AS DATE) AS sales_date,
    COALESCE(stb.quantity, 0) AS quantity_sold,
    GREATEST(COALESCE(stb.sale_amount, 0), 0) AS total_revenue
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) as rn
    FROM sales_transactions_bronze
) stb
WHERE stb.rn = 1
"""

# Execute SQL and save
sales_transactions_silver_df = spark.sql(sales_transactions_silver_query)
sales_transactions_silver_df.write \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")

# --------- Transformations for product_store_mapping_silver ---------

# Create temp views for silver tables
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# Transform SQL
product_store_mapping_silver_query = """
SELECT DISTINCT
    CAST(sts.product_id AS varchar(10)) AS product_id,
    CAST(sts.store_id AS varchar(10)) AS store_id,
    ps.product_name,
    ss.store_name
FROM sales_transactions_silver sts
INNER JOIN products_silver ps ON sts.product_id = ps.product_id
INNER JOIN stores_silver ss ON sts.store_id = ss.store_id
"""

# Execute SQL and save
product_store_mapping_silver_df = spark.sql(product_store_mapping_silver_query)
product_store_mapping_silver_df.write \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/product_store_mapping_silver.csv")

job.commit()
