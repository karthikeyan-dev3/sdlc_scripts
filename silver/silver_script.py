import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ============================================================
# Source Reads (Bronze)
# ============================================================
patient_enrollment_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollment_bronze.{FILE_FORMAT}/")
)
patient_enrollment_bronze_df.createOrReplaceTempView("patient_enrollment_bronze")

# ============================================================
# Target: silver.patient_enrollment_silver
# ============================================================
patient_enrollment_silver_sql = """
WITH standardized AS (
  SELECT
    upper(trim(peb.patient_id)) AS patient_id,
    upper(trim(peb.trial_id)) AS trial_id,
    upper(trim(peb.site_id)) AS site_id,
    upper(trim(peb.country)) AS country,
    cast(peb.enrollment_date as timestamp) AS enrollment_date,
    case
      when upper(trim(peb.consent_status)) in ('CONSENTED','NOT_CONSENTED','WITHDRAWN','UNKNOWN')
        then upper(trim(peb.consent_status))
      else 'UNKNOWN'
    end AS consent_status,
    upper(trim(peb.gender)) AS gender,
    peb.date_of_birth AS date_of_birth,
    peb.source_system AS source_system
  FROM patient_enrollment_bronze peb
),
deduped AS (
  SELECT
    patient_id,
    trial_id,
    site_id,
    country,
    enrollment_date,
    consent_status,
    gender,
    date_of_birth,
    source_system,
    row_number() OVER (
      PARTITION BY patient_id, trial_id, site_id, country, consent_status, gender, date_of_birth
      ORDER BY enrollment_date DESC, source_system DESC
    ) AS rn
  FROM standardized
)
SELECT
  patient_id,
  trial_id,
  site_id,
  country,
  enrollment_date,
  consent_status,
  gender,
  date_of_birth,
  source_system
FROM deduped
WHERE rn = 1
"""

patient_enrollment_silver_df = spark.sql(patient_enrollment_silver_sql)
patient_enrollment_silver_df.createOrReplaceTempView("patient_enrollment_silver")

(
    patient_enrollment_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_enrollment_silver.csv")
)

# ============================================================
# Target: silver.trial_site_rollup_silver
# ============================================================
trial_site_rollup_silver_sql = """
WITH base AS (
  SELECT
    pes.trial_id AS trial_id,
    pes.country AS country,
    pes.site_id AS site_id,
    count(distinct case when pes.consent_status='CONSENTED' then pes.patient_id end) AS total_enrolled_patients,
    count(distinct case when pes.consent_status in ('WITHDRAWN','NOT_CONSENTED') then pes.patient_id end) AS dropout_count,
    count(distinct pes.site_id) over (partition by pes.trial_id, pes.country) AS total_sites,
    current_timestamp() AS metric_aggregation_time
  FROM patient_enrollment_silver pes
  GROUP BY
    pes.trial_id,
    pes.country,
    pes.site_id
)
SELECT
  trial_id,
  country,
  site_id,
  total_enrolled_patients,
  dropout_count,
  total_sites,
  metric_aggregation_time
FROM base
"""

trial_site_rollup_silver_df = spark.sql(trial_site_rollup_silver_sql)
trial_site_rollup_silver_df.createOrReplaceTempView("trial_site_rollup_silver")

(
    trial_site_rollup_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_rollup_silver.csv")
)

# ============================================================
# Target: silver.site_performance_silver
# ============================================================
site_performance_silver_sql = """
SELECT
  tsr.trial_id AS trial_id,
  tsr.country AS country,
  tsr.site_id AS site_id,
  case
    when percent_rank() over (partition by tsr.trial_id, tsr.country order by tsr.total_enrolled_patients asc) <= 0.2
      or (tsr.dropout_count > 0.2 * nullif(tsr.total_enrolled_patients + tsr.dropout_count, 0))
      then true
    else false
  end AS is_underperforming,
  case
    when percent_rank() over (partition by tsr.trial_id, tsr.country order by tsr.total_enrolled_patients desc) <= 0.2
      and (tsr.dropout_count <= 0.1 * nullif(tsr.total_enrolled_patients + tsr.dropout_count, 0))
      then 'HIGH'
    when percent_rank() over (partition by tsr.trial_id, tsr.country order by tsr.total_enrolled_patients asc) <= 0.2
      or (tsr.dropout_count > 0.1 * nullif(tsr.total_enrolled_patients + tsr.dropout_count, 0))
      then 'LOW'
    else 'MEDIUM'
  end AS performance_rating,
  date_trunc('day', tsr.metric_aggregation_time) AS summary_time_period
FROM trial_site_rollup_silver tsr
"""

site_performance_silver_df = spark.sql(site_performance_silver_sql)
site_performance_silver_df.createOrReplaceTempView("site_performance_silver")

(
    site_performance_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/site_performance_silver.csv")
)

# ============================================================
# Target: silver.reporting_metrics_silver
# ============================================================
reporting_metrics_silver_sql = """
WITH base AS (
  SELECT
    current_timestamp() AS update_timestamp
  FROM patient_enrollment_silver pes
  LIMIT 1
),
metrics AS (
  SELECT
    b.update_timestamp AS update_timestamp,
    datediff('minute', max(pes.enrollment_date), b.update_timestamp) AS reporting_latency_minutes,
    100.0 * sum(case when pes.enrollment_date >= b.update_timestamp - interval '60 minutes' then 1 else 0 end) / nullif(count(1), 0) AS data_freshness_percent,
    0.0 AS dashboard_usage_percent,
    100.0 - (
      (100.0 * sum(case when pes.country is null then 1 else 0 end) / nullif(count(1), 0)) * 0.30
      + (100.0 * sum(case when pes.enrollment_date is null then 1 else 0 end) / nullif(count(1), 0)) * 0.40
      + (100.0 * sum(case when pes.consent_status not in ('CONSENTED','NOT_CONSENTED','WITHDRAWN','UNKNOWN') then 1 else 0 end) / nullif(count(1), 0)) * 0.30
    ) AS data_quality_score
  FROM patient_enrollment_silver pes
  CROSS JOIN base b
)
SELECT
  update_timestamp,
  reporting_latency_minutes,
  data_freshness_percent,
  dashboard_usage_percent,
  data_quality_score
FROM metrics
"""

reporting_metrics_silver_df = spark.sql(reporting_metrics_silver_sql)
reporting_metrics_silver_df.createOrReplaceTempView("reporting_metrics_silver")

(
    reporting_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/reporting_metrics_silver.csv")
)

job.commit()