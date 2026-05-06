```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext

# Initialize Spark session and Glue context
spark = SparkSession.builder.appName("AWS Glue Job").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Read source data
sales_transaction_silver = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/sales_transaction_silver.csv/")
product_master_silver = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/product_master_silver.csv/")
store_master_silver = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/store_master_silver.csv/")
store_region_silver = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/store_region_silver.csv/")

# Create temp views
sales_transaction_silver.createOrReplaceTempView("sts")
product_master_silver.createOrReplaceTempView("pms")
store_master_silver.createOrReplaceTempView("sms")
store_region_silver.createOrReplaceTempView("srs")

# Transform and write gold_sales table
gold_sales_df = spark.sql("""
    SELECT
        sts.transaction_id,
        sts.product_id,
        sts.store_id,
        sts.transaction_date,
        sts.sales_amount,
        sts.quantity_sold,
        pms.product_name,
        pms.product_category,
        sms.store_name,
        srs.region
    FROM
        sts
    LEFT JOIN
        pms ON sts.product_id = pms.product_id
    LEFT JOIN
        sms ON sts.store_id = sms.store_id
    LEFT JOIN
        srs ON sts.store_id = srs.store_id
""")
gold_sales_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales.csv", header=True)

# Transform and write gold_store_master table
gold_store_master_df = spark.sql("""
    SELECT
        sms.store_id,
        sms.store_name,
        sms.store_location,
        sms.store_manager
    FROM
        sms
""")
gold_store_master_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_store_master.csv", header=True)

# Transform and write gold_product_master table
gold_product_master_df = spark.sql("""
    SELECT
        pms.product_id,
        pms.product_name,
        pms.product_category,
        pms.manufacturer
    FROM
        pms
""")
gold_product_master_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_product_master.csv", header=True)
```