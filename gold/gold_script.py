```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkConte
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Source: silver.sales_transactions_silver (sts)
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# Target: gold.gold_sales_performance
gold_sales_performance_df = spark.sql(
    """
    SELECT
        CAST(sts.transaction_id AS STRING) AS transaction_id,
        CAST(sts.sale_date AS DATE) AS sale_date,
        CAST(sts.product_id AS STRING) AS product_id,
        CAST(sts.store_id AS STRING) AS store_id,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount,
        CAST(sts.standardized_product_name AS STRING) AS standardized_product_name,
        CAST(sts.standardized_store_name AS STRING) AS standardized_store_name,
        CAST(sts.product_category AS STRING) AS product_category,
        CAST(sts.store_region AS STRING) AS store_region
    FROM sales_transactions_silver sts
    """
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

# ------------------------------------------------------------------------------------
# Source: silver.daily_sales_aggregation_silver (dsas)
# ------------------------------------------------------------------------------------
daily_sales_aggregation_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/daily_sales_aggregation_silver.{FILE_FORMAT}/")
)
daily_sales_aggregation_silver_df.createOrReplaceTempView("daily_sales_aggregation_silver")

# Target: gold.gold_daily_sales_aggregation
gold_daily_sales_aggregation_df = spark.sql(
    """
    SELECT
        CAST(dsas.aggregated_date AS DATE) AS aggregated_date,
        CAST(dsas.total_sales_volume AS INT) AS total_sales_volume,
        CAST(dsas.total_revenue AS DOUBLE) AS total_revenue,
        CAST(dsas.average_sales_value AS DOUBLE) AS average_sales_value,
        CAST(dsas.top_selling_product AS STRING) AS top_selling_product,
        CAST(dsas.top_selling_store AS STRING) AS top_selling_store
    FROM daily_sales_aggregation_silver dsas
    """
)

(
    gold_daily_sales_aggregation_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_daily_sales_aggregation.csv")
)
```