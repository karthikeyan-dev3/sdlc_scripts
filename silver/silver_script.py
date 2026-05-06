import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark = SparkSession.builder \
    .appName("AWS Glue PySpark Application") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

# Transform: sales_transactions_silver
sales_transactions_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
sales_transactions_bronze_df.createOrReplaceTempView("stb")

sales_transactions_silver_df = spark.sql("""
    SELECT 
        stb.transaction_id,
        CAST(stb.transaction_time AS date) AS sale_date,
        stb.store_id,
        stb.product_id,
        stb.quantity AS quantity_sold,
        stb.sale_amount AS sales_amount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
        FROM stb
    ) WHERE rn = 1
""")

sales_transactions_silver_df.drop("rn").write.csv(TARGET_PATH + "/sales_transactions_silver.csv", header=True, mode="overwrite")

# Transform: product_silver
products_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
products_bronze_df.createOrReplaceTempView("pb")

product_silver_df = spark.sql("""
    SELECT 
        pb.product_id, 
        pb.product_name, 
        pb.category, 
        pb.brand
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY CURRENT_TIMESTAMP() DESC) AS rn
        FROM pb
        WHERE COALESCE(is_active, true) = true
    ) WHERE rn = 1
""")

product_silver_df.drop("rn").write.csv(TARGET_PATH + "/product_silver.csv", header=True, mode="overwrite")

# Transform: store_silver
stores_bronze_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
stores_bronze_df.createOrReplaceTempView("sb")

store_silver_df = spark.sql("""
    SELECT 
        sb.store_id, 
        sb.store_name, 
        CONCAT(sb.city, ', ', sb.state) as location, 
        state_to_region(sb.state) AS region
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY CURRENT_TIMESTAMP() DESC) AS rn
        FROM sb
    ) WHERE rn = 1
""")

store_silver_df.drop("rn").write.csv(TARGET_PATH + "/store_silver.csv", header=True, mode="overwrite")

# Transform: sales_daily_aggregate_silver
sales_transactions_silver_df.createOrReplaceTempView("sts")

sales_daily_aggregate_silver_df = spark.sql("""
    SELECT 
        sts.store_id, 
        sts.product_id, 
        SUM(sts.quantity_sold) AS total_quantity_sold, 
        SUM(sts.sales_amount) AS total_sales_amount, 
        sts.sale_date AS reporting_date
    FROM sts
    GROUP BY sts.store_id, sts.product_id, sts.sale_date
""")

sales_daily_aggregate_silver_df.write.csv(TARGET_PATH + "/sales_daily_aggregate_silver.csv", header=True, mode="overwrite")