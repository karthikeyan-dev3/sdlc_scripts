```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, trim, upper, when, cast, current_date
from pyspark.sql.window import Window

# Initialize Spark and Glue
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"

# Products Bronze to Silver Transformation
products_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/products_bronze.csv/")
products_df.createOrReplaceTempView("products_bronze")

products_query = """
SELECT product_id, product_name, category, brand, price
FROM (
    SELECT
        UPPER(TRIM(pb.product_id)) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        CASE WHEN CAST(pb.price AS DOUBLE) < 0 THEN NULL ELSE CAST(pb.price AS DOUBLE) END AS price,
        ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(pb.product_id)) ORDER BY pb.ingest_timestamp DESC) AS row_num
    FROM products_bronze pb
    WHERE pb.is_active = 'true' AND pb.product_id IS NOT NULL
) filtered
WHERE row_num = 1
"""

products_transformed_df = spark.sql(products_query)
products_transformed_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/products_silver.csv", header=True)

# Stores Bronze to Silver Transformation
stores_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.csv/")
stores_df.createOrReplaceTempView("stores_bronze")

stores_query = """
SELECT store_id, store_name, region, store_type, open_date
FROM (
    SELECT
        UPPER(TRIM(sb.store_id)) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CASE
            WHEN TRIM(sb.state) IS NULL THEN NULL
            WHEN UPPER(TRIM(sb.state)) IN ('CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA') THEN 'NORTHEAST'
            WHEN UPPER(TRIM(sb.state)) IN ('IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') THEN 'MIDWEST'
            WHEN UPPER(TRIM(sb.state)) IN ('DE', 'FL', 'GA', 'MD', 'NC', 'SC', 'VA', 'DC', 'WV', 'AL', 'KY', 'MS', 'TN', 'AR', 'LA', 'OK', 'TX') THEN 'SOUTH'
            WHEN UPPER(TRIM(sb.state)) IN ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY', 'AK', 'CA', 'HI', 'OR', 'WA') THEN 'WEST'
            ELSE 'UNKNOWN'
        END AS region,
        TRIM(sb.store_type) AS store_type,
        CASE WHEN sb.open_date > CURRENT_DATE THEN NULL ELSE sb.open_date END AS open_date,
        ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(sb.store_id)) ORDER BY sb.ingest_timestamp DESC) AS row_num
    FROM stores_bronze sb
    WHERE sb.store_id IS NOT NULL
) filtered
WHERE row_num = 1
"""

stores_transformed_df = spark.sql(stores_query)
stores_transformed_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/stores_silver.csv", header=True)

# Sales Transactions Bronze to Silver Transformation
sales_transactions_df = spark.read.format("csv").option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.csv/")
sales_transactions_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_query = """
SELECT transaction_id, transaction_date, product_id, store_id, quantity_sold, total_sales_amount
FROM (
    SELECT
        stb.transaction_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        UPPER(TRIM(stb.product_id)) AS product_id,
        UPPER(TRIM(stb.store_id)) AS store_id,
        CASE WHEN CAST(stb.quantity AS INT) < 0 THEN NULL ELSE CAST(stb.quantity AS INT) END AS quantity_sold,
        CASE WHEN CAST(stb.sale_amount AS DOUBLE) < 0 THEN NULL ELSE CAST(stb.sale_amount AS DOUBLE) END AS total_sales_amount,
        ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS row_num
    FROM sales_transactions_bronze stb
    WHERE stb.transaction_id IS NOT NULL
) filtered
WHERE row_num = 1
"""

sales_transactions_transformed_df = spark.sql(sales_transactions_query)
sales_transactions_transformed_df.drop("row_num").write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)
```