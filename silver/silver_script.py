import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ============================================================
# Read Source Tables (Bronze)
# ============================================================

clinical_trials_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/clinical_trials_bronze.{FILE_FORMAT}/")
)
clinical_trials_bronze_df.createOrReplaceTempView("clinical_trials_bronze")

trial_sites_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/trial_sites_bronze.{FILE_FORMAT}/")
)
trial_sites_bronze_df.createOrReplaceTempView("trial_sites_bronze")

patient_enrollments_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_enrollments_bronze.{FILE_FORMAT}/")
)
patient_enrollments_bronze_df.createOrReplaceTempView("patient_enrollments_bronze")

patient_activities_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_activities_bronze.{FILE_FORMAT}/")
)
patient_activities_bronze_df.createOrReplaceTempView("patient_activities_bronze")

patient_visits_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/patient_visits_bronze.{FILE_FORMAT}/")
)
patient_visits_bronze_df.createOrReplaceTempView("patient_visits_bronze")

# ============================================================
# Table: silver.clinical_trials_silver
# From: bronze.clinical_trials_bronze ctb
# ============================================================

clinical_trials_silver_df = spark.sql(
    """
    SELECT
      *
    FROM (
      SELECT
        ctb.*,
        ROW_NUMBER() OVER (
          PARTITION BY ctb.trial_id
          ORDER BY ctb.trial_id
        ) AS rn
      FROM clinical_trials_bronze ctb
    ) x
    WHERE x.rn = 1
    """
).drop("rn")

clinical_trials_silver_df.createOrReplaceTempView("clinical_trials_silver")

(
    clinical_trials_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/clinical_trials_silver.csv")
)

# ============================================================
# Table: silver.trial_sites_silver
# From: bronze.trial_sites_bronze tsb INNER JOIN silver.clinical_trials_silver cts
# ============================================================

trial_sites_silver_df = spark.sql(
    """
    SELECT
      *
    FROM (
      SELECT
        tsb.*,
        ROW_NUMBER() OVER (
          PARTITION BY tsb.trial_id, tsb.site_id
          ORDER BY tsb.trial_id, tsb.site_id
        ) AS rn
      FROM trial_sites_bronze tsb
      INNER JOIN clinical_trials_silver cts
        ON tsb.trial_id = cts.trial_id
    ) x
    WHERE x.rn = 1
    """
).drop("rn")

trial_sites_silver_df.createOrReplaceTempView("trial_sites_silver")

(
    trial_sites_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_sites_silver.csv")
)

# ============================================================
# Table: silver.patient_enrollments_silver
# From: bronze.patient_enrollments_bronze peb INNER JOIN silver.trial_sites_silver tss
# ============================================================

patient_enrollments_silver_df = spark.sql(
    """
    SELECT
      *
    FROM (
      SELECT
        peb.*,
        ROW_NUMBER() OVER (
          PARTITION BY peb.patient_id, peb.trial_id, peb.site_id, peb.enrollment_date
          ORDER BY peb.patient_id, peb.trial_id, peb.site_id, peb.enrollment_date
        ) AS rn
      FROM patient_enrollments_bronze peb
      INNER JOIN trial_sites_silver tss
        ON peb.trial_id = tss.trial_id
       AND peb.site_id = tss.site_id
    ) x
    WHERE x.rn = 1
    """
).drop("rn")

patient_enrollments_silver_df.createOrReplaceTempView("patient_enrollments_silver")

(
    patient_enrollments_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_enrollments_silver.csv")
)

# ============================================================
# Table: silver.patient_status_silver
# From: bronze.patient_activities_bronze pab INNER JOIN silver.trial_sites_silver tss
# ============================================================

patient_status_silver_df = spark.sql(
    """
    SELECT
      *
    FROM (
      SELECT
        pab.*,
        ROW_NUMBER() OVER (
          PARTITION BY pab.trial_id, pab.site_id, pab.patient_id
          ORDER BY pab.trial_id, pab.site_id, pab.patient_id
        ) AS rn
      FROM patient_activities_bronze pab
      INNER JOIN trial_sites_silver tss
        ON pab.trial_id = tss.trial_id
       AND pab.site_id = tss.site_id
    ) x
    WHERE x.rn = 1
    """
).drop("rn")

patient_status_silver_df.createOrReplaceTempView("patient_status_silver")

(
    patient_status_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_status_silver.csv")
)

# ============================================================
# Table: silver.patient_visits_silver
# From: bronze.patient_visits_bronze pvb INNER JOIN silver.trial_sites_silver tss
# ============================================================

patient_visits_silver_df = spark.sql(
    """
    SELECT
      *
    FROM (
      SELECT
        pvb.*,
        ROW_NUMBER() OVER (
          PARTITION BY pvb.trial_id, pvb.site_id, pvb.patient_id, pvb.visit_id, pvb.visit_date
          ORDER BY pvb.trial_id, pvb.site_id, pvb.patient_id, pvb.visit_id, pvb.visit_date
        ) AS rn
      FROM patient_visits_bronze pvb
      INNER JOIN trial_sites_silver tss
        ON pvb.trial_id = tss.trial_id
       AND pvb.site_id = tss.site_id
    ) x
    WHERE x.rn = 1
    """
).drop("rn")

patient_visits_silver_df.createOrReplaceTempView("patient_visits_silver")

(
    patient_visits_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/patient_visits_silver.csv")
)

# ============================================================
# Table: silver.trial_site_daily_metrics_silver
# From: silver.trial_sites_silver tss
#   LEFT JOIN silver.patient_enrollments_silver pes
#   LEFT JOIN silver.patient_status_silver pss
#   LEFT JOIN silver.patient_visits_silver pvs
# ============================================================

trial_site_daily_metrics_silver_df = spark.sql(
    """
    SELECT
      *
    FROM trial_sites_silver tss
    LEFT JOIN patient_enrollments_silver pes
      ON tss.trial_id = pes.trial_id
     AND tss.site_id = pes.site_id
    LEFT JOIN patient_status_silver pss
      ON tss.trial_id = pss.trial_id
     AND tss.site_id = pss.site_id
    LEFT JOIN patient_visits_silver pvs
      ON tss.trial_id = pvs.trial_id
     AND tss.site_id = pvs.site_id
    """
)

trial_site_daily_metrics_silver_df.createOrReplaceTempView(
    "trial_site_daily_metrics_silver"
)

(
    trial_site_daily_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/trial_site_daily_metrics_silver.csv")
)

job.commit()
