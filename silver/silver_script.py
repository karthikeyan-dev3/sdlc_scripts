from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

# Initialize Glue context and Spark session
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Products Table Transformation and Load
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")

products_df.createOrReplaceTempView("pb")

products_silver = spark.sql("""
    SELECT
        TRIM(UPPER(pb.product_id)) AS product_id,
        TRIM(pb.product_name) AS product_name,
        INITCAP(TRIM(pb.category)) AS product_category,
        pb.brand,
        pb.price,
        pb.is_active,
        ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.is_active DESC, CURRENT_TIMESTAMP() DESC) AS row_num
    FROM
        pb
""").filter("row_num = 1").drop("row_num")

products_silver.write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/products_silver.csv")

# Stores Table Transformation and Load
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")

stores_df.createOrReplaceTempView("sb")

stores_silver = spark.sql("""
    SELECT
        TRIM(UPPER(sb.store_id)) AS store_id,
        TRIM(LOWER(sb.store_type)) AS store_type,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS store_location,
        sb.store_name,
        sb.city,
        sb.state,
        sb.open_date,
        ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.open_date DESC) AS row_num
    FROM
        sb
""").filter("row_num = 1").drop("row_num")

stores_silver.write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/stores_silver.csv")

# Transactions Table Transformation and Load
transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")

transactions_df.createOrReplaceTempView("stb")

transactions_silver = spark.sql("""
    SELECT
        TRIM(UPPER(stb.transaction_id)) AS transaction_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        TRIM(UPPER(stb.product_id)) AS product_id,
        TRIM(UPPER(stb.store_id)) AS store_id,
        CASE WHEN stb.quantity IS NULL OR stb.quantity < 0 THEN 0 ELSE stb.quantity END AS quantity_sold,
        CASE WHEN stb.sale_amount IS NULL OR stb.sale_amount < 0 THEN 0 ELSE stb.sale_amount END AS sales_amount,
        ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS row_num
    FROM
        stb
""").filter("row_num = 1").drop("row_num")

transactions_silver.write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/transactions_silver.csv")

# Retail Sales Enriched Table Transformation and Load
transactions_silver.createOrReplaceTempView("ts")
products_silver.createOrReplaceTempView("ps")
stores_silver.createOrReplaceTempView("ss")

retail_sales_enriched_silver = spark.sql("""
    SELECT
        ts.transaction_id,
        ts.transaction_date,
        ts.product_id,
        ps.product_name,
        ps.product_category,
        ts.store_id,
        ss.store_location,
        ss.store_type,
        ts.quantity_sold,
        ts.sales_amount,
        ts.transaction_date AS aggregation_date
    FROM
        ts
    LEFT JOIN
        ps ON ts.product_id = ps.product_id
    LEFT JOIN
        ss ON ts.store_id = ss.store_id
""")

retail_sales_enriched_silver.write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/retail_sales_enriched_silver.csv")