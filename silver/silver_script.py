from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col, isnull, expr
from pyspark.sql.window import Window

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Load smart_meter_readings_bronze
smart_meter_readings_bronze_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/smart_meter_readings_bronze.{FILE_FORMAT}/")

# Create a temporary view for smart_meter_readings_bronze
smart_meter_readings_bronze_df.createOrReplaceTempView("smart_meter_readings_bronze")

# Transform smart_meter_readings_silver
smart_meter_readings_silver_df = spark.sql("""
    SELECT 
        meter_id,
        CAST(timestamp AS timestamp) AS timestamp,
        energy_consumed_kwh
    FROM (
        SELECT 
            meter_id,
            timestamp,
            energy_consumed_kwh,
            ROW_NUMBER() OVER (PARTITION BY meter_id, timestamp ORDER BY timestamp DESC) as rn
        FROM smart_meter_readings_bronze
        WHERE 
            meter_id IS NOT NULL AND
            timestamp IS NOT NULL AND
            energy_consumed_kwh >= 0
    ) smrb_filtered
    WHERE rn = 1
""")

# Write smart_meter_readings_silver to target
smart_meter_readings_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/smart_meter_readings_silver.csv", header=True)

# Load outage_logs_bronze
outage_logs_bronze_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/outage_logs_bronze.{FILE_FORMAT}/")

# Create a temporary view for outage_logs_bronze
outage_logs_bronze_df.createOrReplaceTempView("outage_logs_bronze")

# Transform outage_event_details_silver
outage_event_details_silver_df = spark.sql("""
    SELECT 
        outage_id,
        UPPER(TRIM(region)) AS region,
        CAST(start_time AS timestamp) AS start_time,
        CAST(end_time AS timestamp) AS end_time,
        COALESCE(duration, UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) AS duration
    FROM (
        SELECT 
            outage_id,
            region,
            start_time,
            end_time,
            duration,
            ROW_NUMBER() OVER (PARTITION BY outage_id ORDER BY start_time DESC) as rn
        FROM outage_logs_bronze
        WHERE 
            outage_id IS NOT NULL AND
            region IS NOT NULL AND
            start_time IS NOT NULL AND
            end_time IS NOT NULL AND
            start_time <= end_time
    ) olb_filtered
    WHERE rn = 1
""")

# Write outage_event_details_silver to target
outage_event_details_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/outage_event_details_silver.csv", header=True)

# Load grid_reliability_reports_bronze
grid_reliability_reports_bronze_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/grid_reliability_reports_bronze.{FILE_FORMAT}/")

# Create a temporary view for grid_reliability_reports_bronze
grid_reliability_reports_bronze_df.createOrReplaceTempView("grid_reliability_reports_bronze")

# Transform grid_reliability_analysis_silver
grid_reliability_analysis_silver_df = spark.sql("""
    SELECT 
        grb.report_date,
        grb.peak_demand,
        COALESCE(grb.outage_zones_high_risk, oeds.region) AS outage_zones_high_risk,
        grb.report_generation_time
    FROM grid_reliability_reports_bronze grb
    LEFT JOIN outage_event_details_silver oeds
    ON CAST(oeds.start_time AS date) = grb.report_date
    WHERE
        grb.peak_demand >= 0
""")

# Write grid_reliability_analysis_silver to target
grid_reliability_analysis_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/grid_reliability_analysis_silver.csv", header=True)

# Load normalized_data_bronze
normalized_data_bronze_df = spark.read\
    .format(FILE_FORMAT)\
    .option("header", "true")\
    .load(f"{SOURCE_PATH}/normalized_data_bronze.{FILE_FORMAT}/")

# Create a temporary view for normalized_data_bronze
normalized_data_bronze_df.createOrReplaceTempView("normalized_data_bronze")

# Transform data_normalization_results_silver
data_normalization_results_silver_df = spark.sql("""
    SELECT 
        TRIM(data_source) AS data_source,
        normalized_value,
        standardized_format
    FROM (
        SELECT 
            data_source,
            normalized_value,
            standardized_format,
            ROW_NUMBER() OVER (PARTITION BY data_source, standardized_format ORDER BY timestamp DESC) as rn
        FROM normalized_data_bronze
        WHERE 
            data_source IS NOT NULL AND
            normalized_value IS NOT NULL AND
            standardized_format IS NOT NULL
    ) ndb_filtered
    WHERE rn = 1
""")

# Write data_normalization_results_silver to target
data_normalization_results_silver_df.coalesce(1).write.mode("overwrite").csv(f"{TARGET_PATH}/data_normalization_results_silver.csv", header=True)