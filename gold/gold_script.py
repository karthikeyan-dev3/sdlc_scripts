import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------
# Source: silver.sales_analysis_silver
# --------------------------------------------------------------------------------
sas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_analysis_silver.{FILE_FORMAT}/")
)
sas_df.createOrReplaceTempView("sales_analysis_silver")

# --------------------------------------------------------------------------------
# Target: gold.sales_analysis
# --------------------------------------------------------------------------------
sales_analysis_df = spark.sql("""
SELECT
    CAST(sas.transaction_id AS STRING)          AS transaction_id,
    DATE(sas.transaction_date)                  AS transaction_date,
    CAST(sas.store_id AS STRING)                AS store_id,
    CAST(sas.product_id AS STRING)              AS product_id,
    CAST(sas.quantity_sold AS INT)              AS quantity_sold,
    CAST(sas.sales_amount AS DOUBLE)            AS sales_amount,
    CAST(sas.store_region AS STRING)            AS store_region,
    CAST(sas.store_manager AS STRING)           AS store_manager,
    CAST(sas.product_category AS STRING)        AS product_category,
    CAST(sas.product_brand AS STRING)           AS product_brand
FROM sales_analysis_silver sas
""")

(
    sales_analysis_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_analysis.csv")
)

# --------------------------------------------------------------------------------
# Target: gold.store_performance
# --------------------------------------------------------------------------------
store_performance_df = spark.sql("""
SELECT
    CAST(sas.store_id AS STRING)                        AS store_id,
    DATE(sas.transaction_date)                          AS transaction_date,
    CAST(SUM(CAST(sas.sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales,
    CAST(SUM(CAST(sas.quantity_sold AS INT)) AS INT)     AS total_units_sold,
    CAST(COUNT(DISTINCT sas.transaction_id) AS INT)      AS number_of_transactions,
    CAST(
        SUM(CAST(sas.sales_amount AS DOUBLE)) / COUNT(DISTINCT sas.transaction_id)
        AS DOUBLE
    ) AS average_transaction_value
FROM sales_analysis_silver sas
GROUP BY
    sas.store_id,
    sas.transaction_date
""")

(
    store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_performance.csv")
)

# --------------------------------------------------------------------------------
# Target: gold.product_performance
# --------------------------------------------------------------------------------
product_performance_df = spark.sql("""
SELECT
    CAST(sas.product_id AS STRING)                        AS product_id,
    DATE(sas.transaction_date)                            AS transaction_date,
    CAST(SUM(CAST(sas.sales_amount AS DOUBLE)) AS DOUBLE) AS total_sales,
    CAST(SUM(CAST(sas.quantity_sold AS INT)) AS INT)       AS total_units_sold,
    CAST(
        SUM(CAST(sas.sales_amount AS DOUBLE)) / SUM(CAST(sas.quantity_sold AS INT))
        AS DOUBLE
    ) AS average_price,
    CAST(COUNT(DISTINCT sas.store_id) AS INT)              AS total_stores_selling
FROM sales_analysis_silver sas
GROUP BY
    sas.product_id,
    sas.transaction_date
""")

(
    product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_performance.csv")
)

job.commit()