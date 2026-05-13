```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, row_number, trim, upper, lower, concat
from pyspark.sql.window import Window

# Initialize SparkSession and GlueContext
spark = SparkSession.builder.appName("GlueJob").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read source tables and create temp views

# Sales Transactions Bronze
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")

sales_transactions_df.createOrReplaceTempView("stb")

# Products Bronze
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")

products_df.createOrReplaceTempView("pb")

# Stores Bronze
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")

stores_df.createOrReplaceTempView("sb")

# Sales Transactions Silver Transformations
sales_transactions_silver_query = """
SELECT sales_id, product_id, store_id, transaction_date, quantity_sold, sales_amount
FROM (
    SELECT
        stb.transaction_id AS sales_id,
        stb.product_id AS product_id,
        stb.store_id AS store_id,
        CAST(stb.transaction_time AS date) AS transaction_date,
        stb.quantity AS quantity_sold,
        stb.sale_amount AS sales_amount,
        ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS row_num
    FROM stb
    WHERE stb.quantity > 0 AND stb.sale_amount >= 0
) filt
WHERE row_num = 1
AND sales_id IS NOT NULL
AND product_id IS NOT NULL
AND store_id IS NOT NULL
AND transaction_date IS NOT NULL
AND quantity_sold IS NOT NULL
AND sales_amount IS NOT NULL
"""

sales_transactions_silver_df = spark.sql(sales_transactions_silver_query)
sales_transactions_silver_df.write.csv(f"{TARGET_PATH}/sales_transactions_silver.csv", mode="overwrite", header=True)

# Products Silver Transformations
products_silver_query = """
SELECT product_id, TRIM(product_name) AS product_name, TRIM(category) AS category, TRIM(brand) AS brand, list_price
FROM (
    SELECT
        pb.product_id AS product_id,
        pb.product_name AS product_name,
        pb.category AS category,
        pb.brand AS brand,
        pb.price AS list_price,
        ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.ingestion_time DESC) AS row_num
    FROM pb
    WHERE pb.is_active = 'true' AND pb.price >= 0
) filt
WHERE row_num = 1
AND product_id IS NOT NULL
AND product_name IS NOT NULL
AND category IS NOT NULL
AND brand IS NOT NULL
AND list_price IS NOT NULL
"""

products_silver_df = spark.sql(products_silver_query)
products_silver_df.write.csv(f"{TARGET_PATH}/products_silver.csv", mode="overwrite", header=True)

# Stores Silver Transformations
stores_silver_query = """
SELECT store_id, TRIM(store_name) AS store_name, CONCAT(TRIM(city), ', ', TRIM(state)) AS location, TRIM(store_type) AS store_type
FROM (
    SELECT
        sb.store_id AS store_id,
        sb.store_name AS store_name,
        sb.city AS city,
        sb.state AS state,
        sb.store_type AS store_type,
        ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.ingestion_time DESC) AS row_num
    FROM sb
) filt
WHERE row_num = 1
AND store_id IS NOT NULL
AND store_name IS NOT NULL
AND location IS NOT NULL
AND store_type IS NOT NULL
"""

stores_silver_df = spark.sql(stores_silver_query)
stores_silver_df.write.csv(f"{TARGET_PATH}/stores_silver.csv", mode="overwrite", header=True)
```