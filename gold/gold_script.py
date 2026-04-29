```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
import sys
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Initialize Spark Session and Glue Context
spark = SparkSession.builder.appName("AWS Glue PySpark").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
sparkContext = glueContext.spark_session

# Paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Load data_pipeline_silver
data_pipeline_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/data_pipeline_silver.{FILE_FORMAT}/")
data_pipeline_silver_df.createOrReplaceTempView("dps")

# Process gold_data_pipeline
gold_data_pipeline_df = spark.sql("""
    SELECT DISTINCT
        dps.pipeline_id,
        dps.source_system,
        dps.ingestion_method,
        dps.sla_commitment,
        dps.transformation_methods,
        dps.last_run_time
    FROM dps
""")
gold_data_pipeline_df.write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(f"{TARGET_PATH}/gold_data_pipeline.csv")

# Load data_governance_policy_silver
data_governance_policy_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/data_governance_policy_silver.{FILE_FORMAT}/")
data_governance_policy_silver_df.createOrReplaceTempView("dgps")

# Process gold_data_governance
gold_data_governance_df = spark.sql("""
    SELECT DISTINCT
        dgps.governance_policy_id,
        dgps.policy_name,
        dgps.compliance_level,
        dgps.validation_rules,
        dgps.last_updated
    FROM dgps
""")
gold_data_governance_df.write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(f"{TARGET_PATH}/gold_data_governance.csv")

# Load data_validation_rule_silver
data_validation_rule_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/data_validation_rule_silver.{FILE_FORMAT}/")
data_validation_rule_silver_df.createOrReplaceTempView("dvrs")

# Process gold_data_validation
gold_data_validation_df = spark.sql("""
    SELECT DISTINCT
        dvrs.validation_id,
        dvrs.field_name,
        dvrs.validation_type,
        dvrs.error_message,
        dvrs.last_validated
    FROM dvrs
""")
gold_data_validation_df.write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(f"{TARGET_PATH}/gold_data_validation.csv")

# Load data_access_api_silver
data_access_api_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/data_access_api_silver.{FILE_FORMAT}/")
data_access_api_silver_df.createOrReplaceTempView("daas")

# Process gold_data_access
gold_data_access_df = spark.sql("""
    SELECT DISTINCT
        daas.api_id,
        daas.endpoint,
        daas.access_level,
        daas.response_time,
        daas.last_accessed
    FROM daas
""")
gold_data_access_df.write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(f"{TARGET_PATH}/gold_data_access.csv")

# Load metadata_catalog_silver
metadata_catalog_silver_df = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/metadata_catalog_silver.{FILE_FORMAT}/")
metadata_catalog_silver_df.createOrReplaceTempView("mcs")

# Process gold_metadata_catalog
gold_metadata_catalog_df = spark.sql("""
    SELECT
        mcs.dataset_id,
        mcs.metadata,
        mcs.lineage,
        mcs.catalog_date,
        mcs.associated_policies,
        dgps.governance_policy_id
    FROM mcs
    LEFT JOIN dgps
    ON mcs.associated_policies LIKE CONCAT('%', dgps.governance_policy_id, '%')
""")
gold_metadata_catalog_df.write.mode("overwrite").format(FILE_FORMAT).option("header", "true").save(f"{TARGET_PATH}/gold_metadata_catalog.csv")
```

This AWS Glue PySpark script reads source tables from S3, performs transformations using Spark SQL, and writes each target table to a single CSV file in the desired gold layer, following the given UDT configurations and transformation logic provided in your input.