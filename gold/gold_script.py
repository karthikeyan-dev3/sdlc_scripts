
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

spark = SparkSession.builder \
    .appName("AWS Glue PySpark Job") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

# Table: gold_sales_transactions

df_sales_transactions = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv/")

df_sales_transactions.createOrReplaceTempView("sts")

gold_sales_transactions_query = """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.transaction_date AS DATE) AS transaction_date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.sales_amount AS DOUBLE) AS sales_amount
    FROM
        sts
"""

gold_sales_transactions_df = spark.sql(gold_sales_transactions_query)

gold_sales_transactions_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales_transactions.csv", header=True)

# Table: gold_product_master

df_product_master = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/product_master_silver.csv/")

df_product_master.createOrReplaceTempView("pms")

gold_product_master_query = """
    SELECT
        CAST(pms.product_id AS STRING) AS product_id,
        TRIM(pms.product_name) AS product_name,
        TRIM(pms.category) AS category,
        TRIM(pms.brand) AS brand,
        CAST(pms.price AS FLOAT) AS price
    FROM
        pms
"""

gold_product_master_df = spark.sql(gold_product_master_query)

gold_product_master_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_product_master.csv", header=True)

# Table: gold_store_master

df_store_master = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/store_master_silver.csv/")

df_store_master.createOrReplaceTempView("sms")

gold_store_master_query = """
    SELECT
        CAST(sms.store_id AS STRING) AS store_id,
        TRIM(sms.store_name) AS store_name,
        TRIM(sms.location) AS location,
        TRIM(sms.region) AS region
    FROM
        sms
"""

gold_store_master_df = spark.sql(gold_store_master_query)

gold_store_master_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_store_master.csv", header=True)

# Table: gold_sales_aggregated

df_sales_aggregated = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/sales_aggregated_daily_silver.csv/")

df_sales_aggregated.createOrReplaceTempView("sads")

gold_sales_aggregated_query = """
    SELECT
        CAST(sads.date AS DATE) AS date,
        CAST(sads.product_id AS STRING) AS product_id,
        CAST(sads.store_id AS STRING) AS store_id,
        CAST(sads.total_quantity_sold AS INT) AS total_quantity_sold,
        CAST(sads.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(sads.average_price AS DOUBLE) AS average_price
    FROM
        sads
"""

gold_sales_aggregated_df = spark.sql(gold_sales_aggregated_query)

gold_sales_aggregated_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales_aggregated.csv", header=True)
