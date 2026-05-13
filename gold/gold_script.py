from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Initialize Glue and Spark contexts
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Define paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold"
FILE_FORMAT = "csv"

# Read and process gold_energy_consumption
smart_meter_readings_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/smart_meter_readings_silver.{FILE_FORMAT}/")

smart_meter_readings_df.createOrReplaceTempView("smrs")

gold_energy_consumption_df = spark.sql("""
    SELECT
        smrs.meter_id,
        smrs.timestamp,
        smrs.energy_consumed_kwh
    FROM smrs
""")

gold_energy_consumption_df.coalesce(1).write \
    .mode('overwrite') \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_energy_consumption.csv")

# Read and process gold_outage_logs
outage_event_details_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/outage_event_details_silver.{FILE_FORMAT}/")

outage_event_details_df.createOrReplaceTempView("oeds")

gold_outage_logs_df = spark.sql("""
    SELECT
        oeds.outage_id,
        oeds.region,
        oeds.start_time,
        oeds.end_time,
        oeds.duration
    FROM oeds
""")

gold_outage_logs_df.coalesce(1).write \
    .mode('overwrite') \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_outage_logs.csv")

# Read and process gold_grid_reliability_report
grid_reliability_analysis_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/grid_reliability_analysis_silver.{FILE_FORMAT}/")

grid_reliability_analysis_df.createOrReplaceTempView("gras")

gold_grid_reliability_report_df = spark.sql("""
    SELECT
        gras.report_date,
        gras.peak_demand,
        gras.outage_zones_high_risk,
        gras.report_generation_time
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY report_date ORDER BY report_generation_time DESC) as rn
        FROM gras
    ) tmp
    WHERE rn = 1
""")

gold_grid_reliability_report_df.coalesce(1).write \
    .mode('overwrite') \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_grid_reliability_report.csv")

# Read and process gold_normalized_data
data_normalization_results_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/data_normalization_results_silver.{FILE_FORMAT}/")

data_normalization_results_df.createOrReplaceTempView("dnrs")

gold_normalized_data_df = spark.sql("""
    SELECT
        data_source,
        normalized_value,
        standardized_format
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY data_source, standardized_format ORDER BY data_source) as rn
        FROM dnrs
    ) tmp
    WHERE rn = 1
""")

gold_normalized_data_df.coalesce(1).write \
    .mode('overwrite') \
    .option("header", "true") \
    .csv(f"{TARGET_PATH}/gold_normalized_data.csv")