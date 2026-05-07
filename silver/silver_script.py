```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, trim, coalesce, lit
from pyspark.sql.window import Window

# Initialize GlueContext and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define file format and paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read sales_transactions_bronze table
sales_transactions_bronze = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze.createOrReplaceTempView("stb")

# Transform and write sales_transactions_silver table
sales_transactions_silver = spark.sql("""
    SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        COALESCE(stb.quantity, 0) AS quantity_sold,
        COALESCE(stb.sale_amount, 0) AS total_sales
    FROM stb
    WHERE 
        stb.transaction_id IS NOT NULL 
        AND COALESCE(stb.quantity, 0) >= 0 
        AND COALESCE(stb.sale_amount, 0) >= 0
""")
sales_transactions_silver.createOrReplaceTempView("sts_temp")

deduped_sales_transactions = spark.sql("""
    SELECT * FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id 
                ORDER BY transaction_date DESC
            ) as rn
        FROM sts_temp
    ) WHERE rn = 1
""").drop("rn")

deduped_sales_transactions.write.mode('overwrite').csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)

# Read stores_bronze table
stores_bronze = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze.createOrReplaceTempView("sb")

# Transform and write stores_silver table
stores_silver = spark.sql("""
    SELECT 
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CASE 
            WHEN TRIM(sb.state) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
            WHEN TRIM(sb.state) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
            WHEN TRIM(sb.state) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
            WHEN TRIM(sb.state) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
            ELSE 'Unknown'
        END AS region
    FROM sb
    WHERE sb.store_id IS NOT NULL
""")
stores_silver.createOrReplaceTempView("ss_temp")

deduped_stores = spark.sql("""
    SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY store_id 
                ORDER BY open_date DESC
            ) as rn
        FROM ss_temp
    ) WHERE rn = 1
""").drop("rn")

deduped_stores.select("store_id", "store_name", "region").write.mode('overwrite').csv(f"{TARGET_PATH}/stores_silver.csv", header=True)

# Read products_bronze table
products_bronze = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze.createOrReplaceTempView("pb")

# Transform and write products_silver table
products_silver = spark.sql("""
    SELECT 
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        COALESCE(pb.is_active, TRUE) AS is_active,
        CASE 
            WHEN pb.price >= 0 THEN pb.price 
            ELSE NULL 
        END AS price
    FROM pb
    WHERE pb.product_id IS NOT NULL
""")
products_silver.createOrReplaceTempView("ps_temp")

deduped_products = spark.sql("""
    SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY product_id 
                ORDER BY is_active DESC, price DESC
            ) as rn
        FROM ps_temp
    ) WHERE rn = 1
""").drop("rn")

deduped_products.select("product_id", "product_name", "category").write.mode('overwrite').csv(f"{TARGET_PATH}/products_silver.csv", header=True)
```