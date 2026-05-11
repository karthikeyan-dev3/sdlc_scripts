from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, row_number, trim, upper, to_date, current_date, expr
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("AWS Glue PySpark Job") \
    .getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Read source tables
sales_transactions_bronze = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/sales_transactions_bronze.csv/")

products_bronze = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/products_bronze.csv/")

stores_bronze = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/stores_bronze.csv/")

# Create temp views
sales_transactions_bronze.createOrReplaceTempView("stb")
products_bronze.createOrReplaceTempView("pb")
stores_bronze.createOrReplaceTempView("sb")

# sales_transactions_silver transformation
sales_transactions_silver_df = spark.sql("""
    SELECT
        stb.transaction_id AS sales_transaction_id,
        stb.product_id,
        stb.store_id,
        CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
        to_date(stb.transaction_time) AS sales_date,
        row_number() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rnk
    FROM stb
""").filter("rnk = 1").drop("rnk")

sales_transactions_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv", header=True)

# products_silver transformation
products_silver_df = spark.sql("""
    SELECT
        TRIM(pb.product_id) AS product_id,
        pb.product_name,
        pb.category AS product_category,
        row_number() OVER (PARTITION BY pb.product_id ORDER BY pb.active DESC) AS rnk
    FROM pb
""").filter("rnk = 1").drop("rnk")

products_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/products_silver.csv", header=True)

# stores_silver transformation
stores_silver_df = spark.sql("""
    SELECT
        TRIM(sb.store_id) AS store_id,
        sb.store_name,
        sb.state AS store_region,
        row_number() OVER (PARTITION BY sb.store_id ORDER BY sb.modified_date DESC) AS rnk
    FROM sb
""").filter("rnk = 1").drop("rnk")

stores_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/stores_silver.csv", header=True)

# sales_performance_enriched_silver transformation
sales_performance_enriched_silver_df = spark.sql("""
    SELECT
        sts.sales_transaction_id,
        sts.product_id,
        sts.store_id,
        sts.sales_amount,
        sts.sales_date,
        ps.product_name,
        ps.product_category,
        ss.store_name,
        ss.store_region,
        CASE WHEN
            sts.sales_transaction_id IS NOT NULL AND
            sts.product_id IS NOT NULL AND
            sts.store_id IS NOT NULL AND
            sts.sales_amount >= 0 AND
            ps.product_id IS NOT NULL AND
            ss.store_id IS NOT NULL
        THEN 'VALID' ELSE 'INVALID' END AS quality_flag,
        CASE WHEN to_date(sts.sales_date) = current_date() THEN 'REFRESHED' ELSE 'STALE' END AS daily_refresh_indicator
    FROM sales_transactions_silver_df AS sts
    LEFT JOIN products_silver_df AS ps ON sts.product_id = ps.product_id
    LEFT JOIN stores_silver_df AS ss ON sts.store_id = ss.store_id
""")

sales_performance_enriched_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/sales_performance_enriched_silver.csv", header=True)