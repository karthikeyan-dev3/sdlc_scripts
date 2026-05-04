```python
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window

glueContext = GlueContext(SparkSession.builder.getOrCreate())
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Processing iot_telemetry_data_silver
iot_telemetry_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/telemetry_event_bronze.{FILE_FORMAT}/")
iot_telemetry_df.createOrReplaceTempView("teb")

iot_telemetry_transformed = spark.sql("""
SELECT
    device_id,
    timestamp,
    CAST(temperature AS DECIMAL(10, 2)) AS temperature,
    CAST(humidity AS DECIMAL(10, 2)) AS humidity,
    CAST(battery_level AS DECIMAL(10, 2)) AS battery_level
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY device_id, timestamp ORDER BY ingestion_timestamp DESC) AS rn
    FROM teb
    WHERE COALESCE(is_deleted, false) = false
)
WHERE rn = 1
""")
iot_telemetry_transformed.write.csv(f"{TARGET_PATH}/iot_telemetry_data_silver.csv", mode="overwrite", header=True)

# Processing drug_stability_thresholds_silver
drug_stability_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/drug_stability_threshold_bronze.{FILE_FORMAT}/")
drug_stability_df.createOrReplaceTempView("dstb")

drug_stability_transformed = spark.sql("""
SELECT
    drug_id,
    CAST(temperature_threshold AS DECIMAL(10, 2)) AS temperature_threshold,
    CAST(humidity_threshold AS DECIMAL(10, 2)) AS humidity_threshold,
    CAST(min_battery_level AS DECIMAL(10, 2)) AS min_battery_level
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY drug_id ORDER BY effective_timestamp DESC, ingestion_timestamp DESC) AS rn
    FROM dstb
    WHERE COALESCE(is_deleted, false) = false
)
WHERE rn = 1
""")
drug_stability_transformed.write.csv(f"{TARGET_PATH}/drug_stability_thresholds_silver.csv", mode="overwrite", header=True)

# Processing telemetry_device_drug_assignment_silver
device_drug_assignment_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/device_drug_assignment_bronze.{FILE_FORMAT}/")
device_drug_assignment_df.createOrReplaceTempView("ddab")

device_drug_assignment_transformed = spark.sql("""
SELECT
    device_id,
    drug_id,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY device_id ORDER BY valid_from) AS valid_to
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY device_id, valid_from ORDER BY ingestion_timestamp DESC) AS rn
    FROM ddab
    WHERE COALESCE(is_deleted, false) = false
)
WHERE rn = 1
""")
device_drug_assignment_transformed.write.csv(f"{TARGET_PATH}/telemetry_device_drug_assignment_silver.csv", mode="overwrite", header=True)

# Processing telemetry_compliance_insights_silver
iot_telemetry_transformed.createOrReplaceTempView("itds")
device_drug_assignment_transformed.createOrReplaceTempView("tddas")
drug_stability_transformed.createOrReplaceTempView("dsts")

telemetry_compliance_insights = spark.sql("""
SELECT
    itds.device_id,
    itds.timestamp,
    itds.temperature,
    itds.humidity,
    itds.battery_level,
    CASE
        WHEN itds.temperature > dsts.temperature_threshold THEN TRUE
        ELSE FALSE
    END AS temperature_threshold_violation,
    CASE
        WHEN itds.humidity > dsts.humidity_threshold THEN TRUE
        ELSE FALSE
    END AS humidity_threshold_violation,
    CASE
        WHEN itds.battery_level < dsts.min_battery_level THEN TRUE
        ELSE FALSE
    END AS battery_level_violation,
    CASE
        WHEN itds.device_id IS NULL THEN 'UNKNOWN'
        WHEN itds.temperature > dsts.temperature_threshold OR
             itds.humidity > dsts.humidity_threshold OR
             itds.battery_level < dsts.min_battery_level THEN 'NON_COMPLIANT'
        ELSE 'COMPLIANT'
    END AS compliance_status
FROM itds
LEFT JOIN tddas ON itds.device_id = tddas.device_id
               AND itds.timestamp >= tddas.valid_from
               AND (tddas.valid_to IS NULL OR itds.timestamp < tddas.valid_to)
LEFT JOIN dsts ON tddas.drug_id = dsts.drug_id
""")
telemetry_compliance_insights.write.csv(f"{TARGET_PATH}/telemetry_compliance_insights_silver.csv", mode="overwrite", header=True)
```