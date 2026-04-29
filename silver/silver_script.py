```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, greatest, expr, row_number
from pyspark.sql.window import Window

# Initialize AWS Glue Spark Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Define temp views for each input source table
pos_sales_event_bronze = spark.read.load(f"{SOURCE_PATH}/pos_sales_event_bronze.{FILE_FORMAT}/")
payment_gateway_payment_event_bronze = spark.read.load(f"{SOURCE_PATH}/payment_gateway_payment_event_bronze.{FILE_FORMAT}/")
inventory_system_inventory_event_bronze = spark.read.load(f"{SOURCE_PATH}/inventory_system_inventory_event_bronze.{FILE_FORMAT}/")
sensor_footfall_event_bronze = spark.read.load(f"{SOURCE_PATH}/sensor_footfall_event_bronze.{FILE_FORMAT}/")

# Create temp views for source tables
pos_sales_event_bronze.createOrReplaceTempView("pseb")
payment_gateway_payment_event_bronze.createOrReplaceTempView("pgpeb")
inventory_system_inventory_event_bronze.createOrReplaceTempView("isieb")
sensor_footfall_event_bronze.createOrReplaceTempView("sfeb")

# Transformation and loading for data_pipeline_silver
data_pipeline_silver = spark.sql("""
    SELECT 
        COALESCE(pseb.source_system, pgpeb.source_system, isieb.source_system, sfeb.source_system) AS source_system,
        GREATEST(pseb.ingestion_timestamp, pgpeb.ingestion_timestamp, isieb.ingestion_timestamp, sfeb.ingestion_timestamp) AS last_run_time
    FROM pseb
    UNION ALL
    SELECT 
        COALESCE(pseb.source_system, pgpeb.source_system, isieb.source_system, sfeb.source_system) AS source_system,
        GREATEST(pseb.ingestion_timestamp, pgpeb.ingestion_timestamp, isieb.ingestion_timestamp, sfeb.ingestion_timestamp) AS last_run_time
    FROM pgpeb
    UNION ALL
    SELECT 
        COALESCE(pseb.source_system, pgpeb.source_system, isieb.source_system, sfeb.source_system) AS source_system,
        GREATEST(pseb.ingestion_timestamp, pgpeb.ingestion_timestamp, isieb.ingestion_timestamp, sfeb.ingestion_timestamp) AS last_run_time
    FROM isieb
    UNION ALL
    SELECT 
        COALESCE(pseb.source_system, pgpeb.source_system, isieb.source_system, sfeb.source_system) AS source_system,
        GREATEST(pseb.ingestion_timestamp, pgpeb.ingestion_timestamp, isieb.ingestion_timestamp, sfeb.ingestion_timestamp) AS last_run_time
    FROM sfeb
""")

# Deduplication using ROW_NUMBER
window_spec = Window.partitionBy("source_system").orderBy(data_pipeline_silver.last_run_time.desc())
data_pipeline_silver_deduped = data_pipeline_silver.withColumn("row_num", row_number().over(window_spec)).filter(expr("row_num = 1")).drop("row_num")

# Save result to target path
data_pipeline_silver_deduped.write.mode("overwrite").csv(f"{TARGET_PATH}/data_pipeline_silver.csv")

# Process for data_governance_policy_silver
data_governance_policy_silver = spark.sql("""
    SELECT 
        governance_policy_id, 
        policy_name, 
        compliance_level, 
        validation_rules,
        last_updated
    FROM VALUES
    (
        ('POLICY_001', 'Privacy Policy', 'High', '{"rule": "data_encryption"}', '2023-10-01')
    ) AS dgp (governance_policy_id, policy_name, compliance_level, validation_rules, last_updated)
""")

# Deduplication for data_governance_policy_silver
window_spec_dgp = Window.partitionBy("governance_policy_id").orderBy(data_governance_policy_silver.last_updated.desc())
data_governance_policy_silver_deduped = data_governance_policy_silver.withColumn("row_num", row_number().over(window_spec_dgp)).filter(expr("row_num = 1")).drop("row_num")

# Save result to target path
data_governance_policy_silver_deduped.write.mode("overwrite").csv(f"{TARGET_PATH}/data_governance_policy_silver.csv")

# Process for data_validation_rule_silver
data_validation_rule_silver = spark.sql("""
    SELECT 
        validation_id,
        field_name,
        validation_type,
        error_message,
        last_validated
    FROM VALUES
    (
        ('VALID_001', 'email', 'format_check', 'Invalid email format', '2023-10-01')
    ) AS dvr (validation_id, field_name, validation_type, error_message, last_validated)
""")

# Deduplication for data_validation_rule_silver
window_spec_dvr = Window.partitionBy("validation_id").orderBy(data_validation_rule_silver.last_validated.desc())
data_validation_rule_silver_deduped = data_validation_rule_silver.withColumn("row_num", row_number().over(window_spec_dvr)).filter(expr("row_num = 1")).drop("row_num")

# Save result to target path
data_validation_rule_silver_deduped.write.mode("overwrite").csv(f"{TARGET_PATH}/data_validation_rule_silver.csv")

# Process for data_access_api_silver
data_access_api_silver = spark.sql("""
    SELECT 
        api_id,
        endpoint,
        access_level,
        response_time,
        last_accessed
    FROM VALUES
    (
        ('API_001', '/user/login', 'Public', 200, '2023-10-01')
    ) AS daa (api_id, endpoint, access_level, response_time, last_accessed)
""")

# Deduplication for data_access_api_silver
window_spec_daa = Window.partitionBy("api_id").orderBy(data_access_api_silver.last_accessed.desc())
data_access_api_silver_deduped = data_access_api_silver.withColumn("row_num", row_number().over(window_spec_daa)).filter(expr("row_num = 1")).drop("row_num")

# Save result to target path
data_access_api_silver_deduped.write.mode("overwrite").csv(f"{TARGET_PATH}/data_access_api_silver.csv")

# Process for metadata_catalog_silver
metadata_catalog_silver = spark.sql("""
    SELECT 
        dataset_id, 
        catalog_date, 
        associated_policies
    FROM VALUES
    (
        ('DS_001', '2023-10-01', '["POLICY_001", "POLICY_002"]')
    ) AS mc (dataset_id, catalog_date, associated_policies)
""")

# Deduplication for metadata_catalog_silver
window_spec_mc = Window.partitionBy("dataset_id").orderBy(metadata_catalog_silver.catalog_date.desc())
metadata_catalog_silver_deduped = metadata_catalog_silver.withColumn("row_num", row_number().over(window_spec_mc)).filter(expr("row_num = 1")).drop("row_num")

# Save result to target path
metadata_catalog_silver_deduped.write.mode("overwrite").csv(f"{TARGET_PATH}/metadata_catalog_silver.csv")
```