```python
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, countDistinct, max
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Initialize GlueContext and SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Read source tables from S3
sales_transactions_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
product_master_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
store_master_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")

# Create temp views
sales_transactions_df.createOrReplaceTempView("sales_transactions_silver")
product_master_df.createOrReplaceTempView("product_master_silver")
store_master_df.createOrReplaceTempView("store_master_silver")

# Gold Sales Transactions
gold_sales_transactions_df = spark.sql("""
    SELECT
        transaction_id,
        store_id,
        product_id,
        transaction_date,
        quantity_sold,
        revenue
    FROM
        sales_transactions_silver
""")
gold_sales_transactions_df.write.csv(f"{TARGET_PATH}/gold_sales_transactions.csv", mode="overwrite", header=True)

# Gold Product Master
gold_product_master_df = spark.sql("""
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM
        product_master_silver
""")
gold_product_master_df.write.csv(f"{TARGET_PATH}/gold_product_master.csv", mode="overwrite", header=True)

# Gold Store Master
gold_store_master_df = spark.sql("""
    SELECT
        store_id,
        store_name,
        region,
        store_type
    FROM
        store_master_silver
""")
gold_store_master_df.write.csv(f"{TARGET_PATH}/gold_store_master.csv", mode="overwrite", header=True)

# Gold Store Performance
gold_store_performance_df = spark.sql("""
    SELECT
        sts.store_id,
        CAST(sts.transaction_date AS DATE) AS date,
        SUM(sts.revenue) AS total_revenue,
        COUNT(DISTINCT sts.transaction_id) AS transaction_count
    FROM
        sales_transactions_silver sts
    LEFT JOIN
        store_master_silver sms ON sts.store_id = sms.store_id
    GROUP BY
        sts.store_id, CAST(sts.transaction_date AS DATE)
""")
gold_store_performance_df.write.csv(f"{TARGET_PATH}/gold_store_performance.csv", mode="overwrite", header=True)

# Gold Product Performance
gold_product_performance_df = spark.sql("""
    SELECT
        sts.product_id,
        CAST(sts.transaction_date AS DATE) AS date,
        SUM(sts.revenue) AS total_revenue,
        SUM(sts.quantity_sold) AS units_sold
    FROM
        sales_transactions_silver sts
    LEFT JOIN
        product_master_silver pms ON sts.product_id = pms.product_id
    GROUP BY
        sts.product_id, CAST(sts.transaction_date AS DATE)
""")
gold_product_performance_df.write.csv(f"{TARGET_PATH}/gold_product_performance.csv", mode="overwrite", header=True)

# Gold Aggregated Sales
best_selling_product_window = Window.partitionBy("date").orderBy(sum("quantity_sold").desc(), sum("revenue").desc(), "product_id")
gold_aggregated_sales_df = spark.sql("""
    SELECT
        CAST(sts.transaction_date AS DATE) AS date,
        SUM(sts.revenue) AS total_revenue,
        COUNT(DISTINCT sts.transaction_id) AS total_transactions,
        FIRST_VALUE(sts.product_id) OVER best_selling_product_window AS best_selling_product
    FROM
        sales_transactions_silver sts
    LEFT JOIN
        product_master_silver pms ON sts.product_id = pms.product_id
    GROUP BY
        CAST(sts.transaction_date AS DATE)
""")
gold_aggregated_sales_df.write.csv(f"{TARGET_PATH}/gold_aggregated_sales.csv", mode="overwrite", header=True)
```