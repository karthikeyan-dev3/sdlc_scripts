```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Gold Sales
sales_enriched_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_enriched_silver.{FILE_FORMAT}/")

sales_enriched_silver_df.createOrReplaceTempView("ses")

gold_sales_df = spark.sql("""
    SELECT
        ses.transaction_id,
        ses.product_id,
        ses.store_id,
        ses.sale_date,
        ses.quantity_sold,
        ses.total_sales_amount,
        ses.store_name,
        ses.store_location,
        ses.product_name,
        ses.product_category
    FROM ses
""")

gold_sales_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_sales.csv")

# Gold Store Performance
store_sales_daily_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/store_sales_daily_silver.{FILE_FORMAT}/")

store_sales_daily_silver_df.createOrReplaceTempView("ssds")

gold_store_performance_df = spark.sql("""
    SELECT
        ssds.store_id,
        ssds.store_name,
        ssds.store_location,
        ssds.total_sales,
        ssds.number_of_transactions,
        ssds.total_sales / ssds.number_of_transactions AS average_transaction_value,
        ssds.date
    FROM ssds
""")

gold_store_performance_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_store_performance.csv")

# Gold Product Performance
product_sales_daily_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/product_sales_daily_silver.{FILE_FORMAT}/")

product_sales_daily_silver_df.createOrReplaceTempView("psds")

gold_product_performance_df = spark.sql("""
    SELECT
        psds.product_id,
        psds.product_name,
        psds.product_category,
        psds.total_sales,
        psds.quantity_sold,
        psds.total_sales / psds.quantity_sold AS average_price,
        psds.date
    FROM psds
""")

gold_product_performance_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_product_performance.csv")

# Gold Aggregated Sales
sales_daily_summary_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_daily_summary_silver.{FILE_FORMAT}/")

sales_daily_summary_silver_df.createOrReplaceTempView("sdss")

gold_aggregated_sales_df = spark.sql("""
    SELECT
        sdss.date,
        sdss.total_sales,
        sdss.total_quantity_sold,
        sdss.number_of_transactions,
        sdss.total_sales / sdss.number_of_transactions AS average_sales_value
    FROM sdss
""")

gold_aggregated_sales_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_aggregated_sales.csv")

# Gold Data Quality Reports
data_quality_metrics_silver_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/data_quality_metrics_silver.{FILE_FORMAT}/")

data_quality_metrics_silver_df.createOrReplaceTempView("dqms")

gold_data_quality_reports_df = spark.sql("""
    SELECT
        dqms.date,
        dqms.total_records_processed,
        dqms.duplicates_removed,
        dqms.errors_detected,
        (dqms.total_records_processed - dqms.duplicates_removed - dqms.errors_detected) / dqms.total_records_processed AS quality_score
    FROM dqms
""")

gold_data_quality_reports_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_data_quality_reports.csv")

job.commit()
```