```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# gold_data_pipeline
data_pipelines_silver = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/data_pipelines_silver.csv/")
data_pipelines_silver.createOrReplaceTempView("dps")

gold_data_pipeline_df = spark.sql("""
    SELECT 
        dps.pipeline_id AS pipeline_id,
        dps.source_name AS source_name,
        dps.ingestion_timestamp AS ingestion_timestamp,
        dps.processing_status AS processing_status
    FROM dps
""")

gold_data_pipeline_df.write.format("csv").mode("overwrite").option("header", "true").save(f"{TARGET_PATH}/gold_data_pipeline.csv")

# gold_data_governance
governance_policies_silver = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/governance_policies_silver.csv/")
governance_policies_silver.createOrReplaceTempView("gps")

gold_data_governance_df = spark.sql("""
    SELECT 
        gps.policy_id AS policy_id,
        gps.policy_name AS policy_name,
        gps.compliance_status AS compliance_status
    FROM gps
""")

gold_data_governance_df.write.format("csv").mode("overwrite").option("header", "true").save(f"{TARGET_PATH}/gold_data_governance.csv")

# gold_sla_management
sla_records_silver = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/sla_records_silver.csv/")
sla_records_silver.createOrReplaceTempView("slas")

gold_sla_management_df = spark.sql("""
    SELECT 
        slas.sla_id AS sla_id,
        slas.data_availability AS data_availability,
        slas.uptime_percentage AS uptime_percentage
    FROM slas
""")

gold_sla_management_df.write.format("csv").mode("overwrite").option("header", "true").save(f"{TARGET_PATH}/gold_sla_management.csv")

# gold_data_validation
validation_rules_silver = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/validation_rules_silver.csv/")
validation_rules_silver.createOrReplaceTempView("vrs")

gold_data_validation_df = spark.sql("""
    SELECT 
        vrs.rule_id AS rule_id,
        vrs.validation_type AS validation_type,
        vrs.status AS status,
        vrs.last_validated AS last_validated
    FROM vrs
""")

gold_data_validation_df.write.format("csv").mode("overwrite").option("header", "true").save(f"{TARGET_PATH}/gold_data_validation.csv")

# gold_metadata_catalog
metadata_catalog_silver = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/metadata_catalog_silver.csv/")
metadata_catalog_silver.createOrReplaceTempView("mcs")

gold_metadata_catalog_df = spark.sql("""
    SELECT 
        mcs.dataset_id AS dataset_id,
        mcs.metadata_details AS metadata_details,
        mcs.last_updated AS last_updated
    FROM mcs
""")

gold_metadata_catalog_df.write.format("csv").mode("overwrite").option("header", "true").save(f"{TARGET_PATH}/gold_metadata_catalog.csv")

# gold_llm_enrichment
enrichment_records_silver = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/enrichment_records_silver.csv/")
enrichment_records_silver.createOrReplaceTempView("ers")

gold_llm_enrichment_df = spark.sql("""
    SELECT 
        ers.dataset_id AS dataset_id,
        ers.enrichment_details AS enrichment_details,
        ers.value_added AS value_added
    FROM ers
""")

gold_llm_enrichment_df.write.format("csv").mode("overwrite").option("header", "true").save(f"{TARGET_PATH}/gold_llm_enrichment.csv")
```