import sys
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Set source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read 'products_bronze' table from S3
products_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")

# Create temporary view for 'products_bronze'
products_bronze_df.createOrReplaceTempView("products_bronze")

# Transform and cleanse 'products_bronze' data
silver_product_details_df = spark.sql("""
    SELECT
        TRIM(pb.product_id) AS product_id,
        NULLIF(TRIM(pb.product_name), '') AS product_name,
        NULLIF(TRIM(pb.category), '') AS product_category,
        CASE WHEN CAST(pb.price AS double) >= 0 THEN CAST(pb.price AS double) ELSE NULL END AS product_price
    FROM 
        products_bronze pb
    WHERE
        pb.is_active = true AND TRIM(pb.product_id) IS NOT NULL
""")

# Deduplicate 'silver_product_details'
window_spec = Window.partitionBy("product_id").orderBy(
    F.col("product_name").desc_nulls_last(),
    F.col("product_category").desc_nulls_last(),
    F.col("product_price").desc_nulls_last()
)

silver_product_details_df = silver_product_details_df \
    .withColumn("rn", F.row_number().over(window_spec)) \
    .filter("rn = 1") \
    .drop("rn")

# Write 'silver_product_details' to S3
silver_product_details_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/silver_product_details.csv")

# Read 'stores_bronze' table from S3
stores_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")

# Create temporary view for 'stores_bronze'
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# Transform and cleanse 'stores_bronze' data
silver_store_details_df = spark.sql("""
    SELECT
        TRIM(sb.store_id) AS store_id,
        CONCAT_WS(', ', NULLIF(TRIM(sb.city), ''), NULLIF(TRIM(sb.state), '')) AS store_location,
        CASE
            WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
            WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
            WHEN UPPER(TRIM(sb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
            WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','OR','UT','WA','WY','CA','AK','HI') THEN 'WEST'
            ELSE 'UNKNOWN'
        END AS store_region,
        NULL AS store_manager
    FROM 
        stores_bronze sb
    WHERE
        TRIM(sb.store_id) IS NOT NULL
""")

# Deduplicate 'silver_store_details'
window_spec_store = Window.partitionBy("store_id").orderBy(
    F.col("store_location").asc_nulls_first()
)

silver_store_details_df = silver_store_details_df \
    .withColumn("rn", F.row_number().over(window_spec_store)) \
    .filter("rn = 1") \
    .drop("rn")

# Write 'silver_store_details' to S3
silver_store_details_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/silver_store_details.csv")

# Read 'sales_transactions_bronze' table from S3
sales_transactions_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")

# Create temporary view for 'sales_transactions_bronze'
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# Transform and cleanse 'sales_transactions_bronze' data
silver_sales_transactions_df = spark.sql("""
    SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        CAST(stb.transaction_time AS timestamp) AS transaction_datetime,
        CAST(stb.transaction_time AS date) AS transaction_date,
        TRIM(stb.product_id) AS product_id,
        TRIM(stb.store_id) AS store_id,
        CASE WHEN CAST(stb.quantity AS int) > 0 THEN CAST(stb.quantity AS int) ELSE NULL END AS quantity,
        CASE WHEN CAST(stb.sale_amount AS double) >= 0 THEN CAST(stb.sale_amount AS double) ELSE NULL END AS amount
    FROM 
        sales_transactions_bronze stb
    WHERE
        TRIM(stb.transaction_id) IS NOT NULL
""")

# Deduplicate 'silver_sales_transactions'
window_spec_sales = Window.partitionBy("transaction_id").orderBy(
    F.col("transaction_datetime").desc(),
    F.col("amount").desc_nulls_last(),
    F.col("quantity").desc_nulls_last()
)

silver_sales_transactions_df = silver_sales_transactions_df \
    .withColumn("rn", F.row_number().over(window_spec_sales)) \
    .filter("rn = 1 AND (quantity IS NOT NULL OR amount IS NOT NULL)") \
    .drop("rn")

# Write 'silver_sales_transactions' to S3
silver_sales_transactions_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/silver_sales_transactions.csv")

# Create temporary views for silver tables
silver_product_details_df.createOrReplaceTempView("silver_product_details")
silver_store_details_df.createOrReplaceTempView("silver_store_details")
silver_sales_transactions_df.createOrReplaceTempView("silver_sales_transactions")

# Enriched sales data by joining product and store details
silver_sales_enrichment_df = spark.sql("""
    SELECT
        sst.transaction_id,
        sst.transaction_date,
        sst.product_id,
        sst.store_id,
        sst.amount AS sales_amount,
        sst.quantity AS quantity_sold,
        COALESCE(spd.product_category, 'UNKNOWN') AS product_category,
        COALESCE(ssd.store_region, 'UNKNOWN') AS store_region
    FROM
        silver_sales_transactions sst
    LEFT JOIN
        silver_product_details spd ON sst.product_id = spd.product_id
    LEFT JOIN
        silver_store_details ssd ON sst.store_id = ssd.store_id
""")

# Write enriched sales data to S3
silver_sales_enrichment_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/silver_sales_enrichment.csv")