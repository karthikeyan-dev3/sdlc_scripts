```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read source table: iot_telemetry_data_silver
iot_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/iot_telemetry_data_silver.csv/")
iot_df.createOrReplaceTempView("iot_telemetry_data_silver")

# Transform and write target table: gold_telemetry_data
gold_telemetry_data = spark.sql("""
    SELECT
        device_id,
        timestamp,
        temperature,
        humidity,
        battery_level
    FROM iot_telemetry_data_silver
""")
gold_telemetry_data.write.mode('overwrite').option("header", "true").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_telemetry_data.csv")

# Read source table: drug_stability_thresholds_silver
drug_stability_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/drug_stability_thresholds_silver.csv/")
drug_stability_df.createOrReplaceTempView("drug_stability_thresholds_silver")

# Transform and write target table: gold_drug_stability_thresholds
gold_drug_stability_thresholds = spark.sql("""
    SELECT
        drug_id,
        temperature_threshold,
        humidity_threshold,
        min_battery_level
    FROM drug_stability_thresholds_silver
""")
gold_drug_stability_thresholds.write.mode('overwrite').option("header", "true").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_drug_stability_thresholds.csv")

# Read source table: telemetry_compliance_insights_silver
compliance_insights_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/silver/telemetry_compliance_insights_silver.csv/")
compliance_insights_df.createOrReplaceTempView("telemetry_compliance_insights_silver")

# Transform and write target table: gold_insights
gold_insights = spark.sql("""
    SELECT
        device_id,
        timestamp,
        temperature,
        humidity,
        battery_level,
        temperature_threshold_violation,
        humidity_threshold_violation,
        battery_level_violation,
        compliance_status
    FROM telemetry_compliance_insights_silver
""")
gold_insights.write.mode('overwrite').option("header", "true").csv("s3://sdlc-agent-bucket/engineering-agent/gold/gold_insights.csv")
```