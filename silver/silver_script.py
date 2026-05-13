from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, row_number
from pyspark.sql.window import Window

# Initialize GlueContext and SparkSession
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read bronze_gps_logs_bronze
bronze_gps_logs_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/bronze_gps_logs_bronze.{FILE_FORMAT}/")

bronze_gps_logs_bronze_df.createOrReplaceTempView("glb")

# Read bronze_fuel_transactions_bronze
bronze_fuel_transactions_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/bronze_fuel_transactions_bronze.{FILE_FORMAT}/")

bronze_fuel_transactions_bronze_df.createOrReplaceTempView("ftb")

# Read bronze_maintenance_records_bronze
bronze_maintenance_records_bronze_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/bronze_maintenance_records_bronze.{FILE_FORMAT}/")

bronze_maintenance_records_bronze_df.createOrReplaceTempView("mrb")

# Vehicles Silver transformation
vehicles_silver_df = spark.sql("""
    SELECT 
        COALESCE(glb.vehicle_id, ftb.vehicle_id, mrb.vehicle_id) as vehicle_id
    FROM 
        glb 
        FULL OUTER JOIN ftb ON glb.vehicle_id = ftb.vehicle_id
        FULL OUTER JOIN mrb ON COALESCE(glb.vehicle_id, ftb.vehicle_id) = mrb.vehicle_id
    """)
vehicles_silver_df.write.csv(f"{TARGET_PATH}/vehicles_silver.csv", header=True, mode="overwrite")

# GPS Logs Silver transformation
gps_logs_silver = spark.sql("""
    SELECT 
        glb.log_id,
        glb.vehicle_id,
        glb.latitude,
        glb.longitude,
        glb.speed,
        glb.log_timestamp
    FROM 
        (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY vehicle_id, log_timestamp ORDER BY log_id) as row_num
            FROM glb
        ) subquery
    WHERE row_num = 1
    """)
gps_logs_silver.write.csv(f"{TARGET_PATH}/gps_logs_silver.csv", header=True, mode="overwrite")

# Fuel Transactions Silver transformation
fuel_transactions_silver = spark.sql("""
    SELECT 
        ftb.transaction_id,
        ftb.vehicle_id,
        ftb.fuel_quantity,
        TRIM(UPPER(ftb.fuel_vendor)) as fuel_vendor,
        ftb.transaction_date
    FROM 
        (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY vehicle_id) as row_num
            FROM ftb
            WHERE COALESCE(fuel_quantity, 0) >= 0
        ) subquery
    WHERE row_num = 1
    """)
fuel_transactions_silver.write.csv(f"{TARGET_PATH}/fuel_transactions_silver.csv", header=True, mode="overwrite")

# Maintenance Records Silver transformation
maintenance_records_silver = spark.sql("""
    SELECT 
        mrb.maintenance_id,
        mrb.vehicle_id,
        mrb.maintenance_type,
        mrb.maintenance_date,
        mrb.maintenance_cost,
        mrb.next_maintenance_due
    FROM 
        (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY maintenance_id ORDER BY vehicle_id) as row_num
            FROM mrb
            WHERE COALESCE(maintenance_cost, 0) >= 0
        ) subquery
    WHERE row_num = 1
    """)
maintenance_records_silver.write.csv(f"{TARGET_PATH}/maintenance_records_silver.csv", header=True, mode="overwrite")

# Fleet Events Silver transformation
fleet_events_silver = spark.sql("""
    SELECT 
        gls.log_id,
        gls.vehicle_id,
        gls.latitude,
        gls.longitude,
        gls.speed,
        gls.log_timestamp,
        fts.transaction_id,
        fts.fuel_quantity,
        fts.transaction_date,
        fts.fuel_vendor,
        mrs.maintenance_id,
        mrs.maintenance_type,
        mrs.maintenance_date,
        mrs.maintenance_cost,
        mrs.next_maintenance_due
    FROM 
        gps_logs_silver gls 
        LEFT JOIN fuel_transactions_silver fts ON gls.vehicle_id = fts.vehicle_id AND DATE_TRUNC('hour', gls.log_timestamp) = DATE_TRUNC('hour', fts.transaction_date)
        LEFT JOIN maintenance_records_silver mrs ON gls.vehicle_id = mrs.vehicle_id AND DATE(gls.log_timestamp) = DATE(mrs.maintenance_date)
    """)
fleet_events_silver.write.csv(f"{TARGET_PATH}/fleet_events_silver.csv", header=True, mode="overwrite")