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

# ------------------------------------------------------------
# Read Source Tables (CSV)
# ------------------------------------------------------------
customers_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customers_bronze.{FILE_FORMAT}/")
)

branches_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branches_bronze.{FILE_FORMAT}/")
)

loan_applications_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_applications_bronze.{FILE_FORMAT}/")
)

loan_risk_assessment_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_risk_assessment_bronze.{FILE_FORMAT}/")
)

repayment_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/repayment_transactions_bronze.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# Temp Views
# ------------------------------------------------------------
customers_bronze_df.createOrReplaceTempView("customers_bronze")
branches_bronze_df.createOrReplaceTempView("branches_bronze")
loan_applications_bronze_df.createOrReplaceTempView("loan_applications_bronze")
loan_risk_assessment_bronze_df.createOrReplaceTempView("loan_risk_assessment_bronze")
repayment_transactions_bronze_df.createOrReplaceTempView("repayment_transactions_bronze")

# ------------------------------------------------------------
# customers_silver
# ------------------------------------------------------------
customers_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(cb.customer_id) AS STRING) AS customer_id,
        CAST(TRIM(cb.customer_name) AS STRING) AS customer_name,
        CAST(TRIM(cb.income_segment) AS STRING) AS customer_segment,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(cb.customer_id)
          ORDER BY
            CASE WHEN cb.customer_name IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN cb.income_segment IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM customers_bronze cb
      WHERE cb.customer_id IS NOT NULL
    )
    SELECT
      CAST(customer_id AS STRING) AS customer_id,
      CAST(customer_name AS STRING) AS customer_name,
      CAST(customer_segment AS STRING) AS customer_segment
    FROM base
    WHERE rn = 1
    """
)

(
    customers_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/customers_silver.csv")
)

# ------------------------------------------------------------
# branches_silver
# ------------------------------------------------------------
branches_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(bb.branch_id) AS STRING) AS branch_id,
        CAST(TRIM(bb.branch_name) AS STRING) AS branch_name,
        CAST(TRIM(bb.branch_id) AS STRING) AS cost_center,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(bb.branch_id)
          ORDER BY
            CASE WHEN bb.branch_name IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM branches_bronze bb
      WHERE bb.branch_id IS NOT NULL
    )
    SELECT
      CAST(branch_id AS STRING) AS branch_id,
      CAST(branch_name AS STRING) AS branch_name,
      CAST(cost_center AS STRING) AS cost_center
    FROM base
    WHERE rn = 1
    """
)

(
    branches_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/branches_silver.csv")
)

# ------------------------------------------------------------
# loans_silver
# ------------------------------------------------------------
loans_silver_df = spark.sql(
    """
    WITH lra_max AS (
      SELECT
        loan_id,
        MAX(evaluation_date) AS latest_evaluation_date
      FROM loan_risk_assessment_bronze
      GROUP BY loan_id
    ),
    rta AS (
      SELECT
        loan_id,
        SUM(CASE WHEN payment_status IN ('SUCCESS','COMPLETED','PAID') THEN CAST(payment_amount AS INT) ELSE 0 END) AS amount_repaid,
        MAX(payment_date) AS last_payment_date,
        MIN(CASE WHEN payment_status IN ('SUCCESS','COMPLETED','PAID') THEN payment_date END) AS first_success_payment_date
      FROM repayment_transactions_bronze
      GROUP BY loan_id
    ),
    joined AS (
      SELECT
        lab.loan_id,
        lab.customer_id,
        lab.branch_id,
        CAST(lab.loan_amount AS INT) AS loan_amount,
        CAST(lab.interest_rate AS FLOAT) AS interest_rate,
        lab.loan_status,
        lrab.loan_id AS lrab_loan_id,
        lrab.evaluation_date AS lrab_evaluation_date,
        CAST(rta.amount_repaid AS INT) AS amount_repaid,
        rta.last_payment_date AS repayment_date
      FROM loan_applications_bronze lab
      LEFT JOIN lra_max
        ON lab.loan_id = lra_max.loan_id
      LEFT JOIN loan_risk_assessment_bronze lrab
        ON lab.loan_id = lrab.loan_id
       AND lrab.evaluation_date = lra_max.latest_evaluation_date
      LEFT JOIN rta
        ON lab.loan_id = rta.loan_id
    ),
    dedup AS (
      SELECT
        CAST(TRIM(loan_id) AS STRING) AS loan_id,
        CAST(TRIM(customer_id) AS STRING) AS customer_id,
        CAST(TRIM(branch_id) AS STRING) AS branch_id,
        CAST(loan_amount AS INT) AS loan_amount,
        CAST(interest_rate AS FLOAT) AS interest_rate,
        CAST(TRIM(loan_status) AS STRING) AS loan_status,
        CAST(COALESCE(amount_repaid, 0) AS INT) AS amount_repaid,
        CAST((loan_amount - COALESCE(amount_repaid, 0)) AS INT) AS outstanding_balance,
        CAST(repayment_date AS DATE) AS repayment_date,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(loan_id)
          ORDER BY
            CASE WHEN loan_amount IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN interest_rate IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN loan_status IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM joined
      WHERE loan_id IS NOT NULL
    )
    SELECT
      CAST(loan_id AS STRING) AS loan_id,
      CAST(customer_id AS STRING) AS customer_id,
      CAST(branch_id AS STRING) AS branch_id,
      CAST(loan_amount AS INT) AS loan_amount,
      CAST(interest_rate AS FLOAT) AS interest_rate,
      CAST(loan_status AS STRING) AS loan_status,
      CAST(amount_repaid AS INT) AS amount_repaid,
      CAST(outstanding_balance AS INT) AS outstanding_balance,
      CAST(repayment_date AS DATE) AS repayment_date
    FROM dedup
    WHERE rn = 1
    """
)

(
    loans_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/loans_silver.csv")
)

job.commit()
