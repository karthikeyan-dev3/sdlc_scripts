from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Reading and processing for gold_gps_logs
gps_logs_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/gps_logs_silver.csv/")
gps_logs_df.createOrReplaceTempView("gls")

gold_gps_logs_df = spark.sql("""
SELECT 
    gls.log_id,
    gls.vehicle_id,
    gls.latitude,
    gls.longitude,
    gls.speed,
    gls.log_timestamp
FROM gls
""")
gold_gps_logs_df.write.csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_gps_logs.csv", header=True, mode="overwrite")

# Reading and processing for gold_fuel_transactions
fuel_transactions_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/fuel_transactions_silver.csv/")
fuel_transactions_df.createOrReplaceTempView("fts")

gold_fuel_transactions_df = spark.sql("""
SELECT 
    fts.transaction_id,
    fts.vehicle_id,
    fts.fuel_quantity,
    fts.transaction_date,
    fts.fuel_vendor
FROM fts
""")
gold_fuel_transactions_df.write.csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_fuel_transactions.csv", header=True, mode="overwrite")

# Reading and processing for gold_maintenance_records
maintenance_records_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/maintenance_records_silver.csv/")
maintenance_records_df.createOrReplaceTempView("mrs")

gold_maintenance_records_df = spark.sql("""
SELECT 
    mrs.maintenance_id,
    mrs.vehicle_id,
    mrs.maintenance_type,
    mrs.maintenance_date,
    mrs.maintenance_cost,
    mrs.next_maintenance_due
FROM mrs
""")
gold_maintenance_records_df.write.csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_maintenance_records.csv", header=True, mode="overwrite")

# Reading and processing for gold_fleet_data
fleet_events_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/fleet_events_silver.csv/")
fleet_events_df.createOrReplaceTempView("fes")

vehicles_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(f"s3://sdlc-agent-bucket/engineering-agent/silver/vehicles_silver.csv/")
vehicles_df.createOrReplaceTempView("vs")

gold_fleet_data_df = spark.sql("""
SELECT 
    vs.vehicle_id,
    fes.log_timestamp AS timestamp,
    fes.latitude AS vehicle_location_latitude,
    fes.longitude AS vehicle_location_longitude,
    fes.fuel_quantity AS fuel_consumption
FROM fes
LEFT JOIN vs ON fes.vehicle_id = vs.vehicle_id
""")
gold_fleet_data_df.write.csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_fleet_data.csv", header=True, mode="overwrite")