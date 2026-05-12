```python
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = SparkSession.builder.getOrCreate()

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables
product_details_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")
store_information_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/store_information_silver.{FILE_FORMAT}/")
sales_transactions_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
aggregated_sales_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")

# Create temp views
product_details_df.createOrReplaceTempView("product_details_silver")
store_information_df.createOrReplaceTempView("store_information_silver")
sales_transactions_df.createOrReplaceTempView("sales_transactions_silver")
aggregated_sales_df.createOrReplaceTempView("aggregated_sales_silver")

# Process gold_product_master
gold_product_master_df = spark.sql("""
    SELECT
        pds.product_id,
        pds.product_name,
        pds.category,
        pds.brand,
        pds.price
    FROM
        product_details_silver pds
""")
gold_product_master_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_product_master.csv", header=True)

# Process gold_store_master
gold_store_master_df = spark.sql("""
    SELECT
        sis.store_id,
        sis.store_name,
        sis.location,
        sis.region,
        sis.store_type
    FROM
        store_information_silver sis
""")
gold_store_master_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_store_master.csv", header=True)

# Process gold_sales_transactions
gold_sales_transactions_df = spark.sql("""
    SELECT
        sts.transaction_id,
        sts.product_id,
        sts.store_id,
        sts.sales_date,
        sts.quantity_sold,
        sts.total_sales_amount
    FROM
        sales_transactions_silver sts
    INNER JOIN
        product_details_silver pds ON sts.product_id = pds.product_id
    INNER JOIN
        store_information_silver sis ON sts.store_id = sis.store_id
""")
gold_sales_transactions_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_transactions.csv", header=True)

# Process gold_sales_aggregated
gold_sales_aggregated_df = spark.sql("""
    SELECT
        ass.sales_date,
        ass.region,
        ass.total_sales_amount,
        ass.total_quantity_sold,
        ass.average_sales_per_store
    FROM
        aggregated_sales_silver ass
""")
gold_sales_aggregated_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_sales_aggregated.csv", header=True)
```