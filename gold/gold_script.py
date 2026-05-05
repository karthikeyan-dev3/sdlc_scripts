```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read source tables from S3

# sales_transactions_silver
sales_transactions_silver_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv/")
)
sales_transactions_silver_df.createOrReplaceTempView("sts")

# product_master_silver
product_master_silver_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/product_master_silver.csv/")
)
product_master_silver_df.createOrReplaceTempView("pms")

# Transform and Write gold_sales_transactions
gold_sales_transactions_df = spark.sql("""
    SELECT 
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        sales_amount
    FROM sts
""")
gold_sales_transactions_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales_transactions.csv")

# Transform and Write gold_product_master
gold_product_master_df = spark.sql("""
    SELECT 
        product_id,
        product_name,
        category,
        brand,
        price
    FROM pms
""")
gold_product_master_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_product_master.csv")

# Transform and Write gold_sales_aggregated
gold_sales_aggregated_df = spark.sql("""
    SELECT 
        to_date(transaction_date) AS aggregation_date,
        store_id,
        product_id,
        SUM(quantity_sold) AS total_quantity_sold,
        SUM(sales_amount) AS total_sales_amount
    FROM sts
    GROUP BY to_date(transaction_date), store_id, product_id
""")
gold_sales_aggregated_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales_aggregated.csv")
```