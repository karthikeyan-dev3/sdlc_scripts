import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Initialize GlueContext and SparkSession
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Process gold_inventory_usage
inventory_usage_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/inventory_usage_silver.{FILE_FORMAT}/")
)

inventory_usage_df.createOrReplaceTempView("ius")

gold_inventory_usage_df = spark.sql("""
    SELECT 
        ius.ingredient_id,
        ius.date,
        ius.quantity_used,
        COALESCE(ius.waste_quantity, 0.0) as waste_quantity,
        ius.location_id
    FROM ius
""")

gold_inventory_usage_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_inventory_usage.csv", header=True)

# Process gold_supplier_performance
supplier_performance_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/supplier_performance_silver.{FILE_FORMAT}/")
)

supplier_performance_df.createOrReplaceTempView("sps")

gold_supplier_performance_df = spark.sql("""
    SELECT 
        sps.supplier_id,
        sps.delivery_date,
        sps.quantity_delivered,
        sps.on_time_delivery
    FROM sps
""")

gold_supplier_performance_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_supplier_performance.csv", header=True)

# Process gold_spoilage_trends
spoilage_trends_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/spoilage_trends_silver.{FILE_FORMAT}/")
)

spoilage_trends_df.createOrReplaceTempView("sts")

gold_spoilage_trends_df = spark.sql("""
    SELECT 
        sts.ingredient_id,
        sts.spoilage_date,
        sts.waste_quantity,
        sts.reason_for_spoilage
    FROM sts
""")

gold_spoilage_trends_df.write.mode("overwrite").csv(f"{TARGET_PATH}/gold_spoilage_trends.csv", header=True)

# Commit the job
job.commit()