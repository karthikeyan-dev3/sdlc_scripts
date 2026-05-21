from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("silver_gold_job", {})

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"
WRITE_MODE = "overwrite"

# ============================================================
# SOURCE TABLE READS + TEMP VIEWS (BRONZE)
# ============================================================

branch_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branch_master_bronze.{FILE_FORMAT}/")
)
branch_master_bronze_df.createOrReplaceTempView("branch_master_bronze")

customer_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customer_master_bronze.{FILE_FORMAT}/")
)
customer_master_bronze_df.createOrReplaceTempView("customer_master_bronze")

loan_applications_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_applications_bronze.{FILE_FORMAT}/")
)
loan_applications_bronze_df.createOrReplaceTempView("loan_applications_bronze")

repayment_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/repayment_transactions_bronze.{FILE_FORMAT}/")
)
repayment_transactions_bronze_df.createOrReplaceTempView("repayment_transactions_bronze")

# ============================================================
# TARGET: silver.branch_master_silver
# ============================================================

branch_master_silver_sql = """
SELECT
  CAST(bmb.branch_id AS STRING) AS branch_id,
  CAST(bmb.branch_name AS STRING) AS branch_name,
  CONCAT(CAST(bmb.city AS STRING), ', ', CAST(bmb.state AS STRING)) AS branch_location,
  CAST(bmb.manager_name AS STRING) AS branch_manager
FROM (
  SELECT
    bmb.*,
    ROW_NUMBER() OVER (
      PARTITION BY bmb.branch_id
      ORDER BY CAST(bmb.opening_year AS INT) DESC, bmb.branch_name DESC
    ) AS rn
  FROM branch_master_bronze bmb
) bmb
WHERE bmb.rn = 1
"""

branch_master_silver_df = spark.sql(branch_master_silver_sql)

branch_master_silver_output_path = f"{TARGET_PATH}/branch_master_silver.csv"
(
    branch_master_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(branch_master_silver_output_path)
)

branch_master_silver_df.createOrReplaceTempView("loan_branch_master_silver_ignore")  # no-op view guard
branch_master_silver_df.createOrReplaceTempView("branch_master_silver")

# ============================================================
# TARGET: silver.customers_silver
# ============================================================

customers_silver_sql = """
SELECT
  CAST(cmb.customer_id AS STRING) AS customer_id,
  TRIM(CAST(cmb.customer_name AS STRING)) AS customer_name,
  CAST(NULL AS INT) AS customer_age,
  CAST(cmb.annual_income AS INT) AS customer_income,
  CAST(cmb.credit_score AS INT) AS credit_score
FROM (
  SELECT
    cmb.*,
    ROW_NUMBER() OVER (
      PARTITION BY cmb.customer_id
      ORDER BY CAST(cmb.credit_score AS INT) DESC, CAST(cmb.annual_income AS INT) DESC
    ) AS rn
  FROM customer_master_bronze cmb
) cmb
WHERE cmb.rn = 1
"""

customers_silver_df = spark.sql(customers_silver_sql)

customers_silver_output_path = f"{TARGET_PATH}/customers_silver.csv"
(
    customers_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(customers_silver_output_path)
)

customers_silver_df.createOrReplaceTempView("customers_silver")

# ============================================================
# TARGET: silver.loan_agreements_silver
# ============================================================

loan_agreements_silver_sql = """
SELECT
  CAST(lab.loan_id AS STRING) AS loan_id,
  CAST(lab.customer_id AS STRING) AS customer_id,
  CAST(lab.branch_id AS STRING) AS branch_id,
  CAST(lab.interest_rate AS FLOAT) AS interest_rate,
  DATE(CAST(lab.application_date AS STRING)) AS application_date,
  CAST(lab.loan_amount AS INT) AS loan_amount,
  CAST(lab.loan_status AS STRING) AS loan_status,
  CAST(lab.loan_type AS STRING) AS loan_type
FROM (
  SELECT
    lab.*,
    ROW_NUMBER() OVER (
      PARTITION BY lab.loan_id
      ORDER BY DATE(CAST(lab.application_date AS STRING)) DESC
    ) AS rn
  FROM loan_applications_bronze lab
) lab
WHERE lab.rn = 1
"""

loan_agreements_silver_df = spark.sql(loan_agreements_silver_sql)

loan_agreements_silver_output_path = f"{TARGET_PATH}/loan_agreements_silver.csv"
(
    loan_agreements_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(loan_agreements_silver_output_path)
)

loan_agreements_silver_df.createOrReplaceTempView("loan_agreements_silver")

# ============================================================
# TARGET: silver.loan_transactions_silver
# ============================================================

loan_transactions_silver_sql = """
SELECT
  CAST(rtb.transaction_id AS STRING) AS transaction_id,
  CAST(rtb.loan_id AS STRING) AS loan_id,
  CAST(las.customer_id AS STRING) AS customer_id,
  CAST(las.branch_id AS STRING) AS branch_id,
  DATE(CAST(rtb.payment_date AS STRING)) AS transaction_date,
  CAST(rtb.payment_amount AS INT) AS amount,
  CAST(las.interest_rate AS FLOAT) AS interest_rate,
  CAST('repayment' AS STRING) AS transaction_type
FROM (
  SELECT
    rtb.*,
    ROW_NUMBER() OVER (
      PARTITION BY rtb.transaction_id
      ORDER BY DATE(CAST(rtb.payment_date AS STRING)) DESC
    ) AS rn
  FROM repayment_transactions_bronze rtb
  WHERE rtb.transaction_id IS NOT NULL
) rtb
LEFT JOIN loan_agreements_silver las
  ON rtb.loan_id = las.loan_id
WHERE rtb.rn = 1
"""

loan_transactions_silver_df = spark.sql(loan_transactions_silver_sql)

loan_transactions_silver_output_path = f"{TARGET_PATH}/loan_transactions_silver.csv"
(
    loan_transactions_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(loan_transactions_silver_output_path)
)

loan_transactions_silver_df.createOrReplaceTempView("loan_transactions_silver")

# ============================================================
# TARGET: silver.loan_analytics_silver
# ============================================================

loan_analytics_silver_sql = """
SELECT
  CAST(lts.branch_id AS STRING) AS branch_id,
  CAST(lts.customer_id AS STRING) AS customer_id,
  CAST(COUNT(DISTINCT lts.loan_id) AS INT) AS total_loans,
  CAST(SUM(la.loan_amount) AS INT) AS total_disbursed,
  CAST(SUM(lts.amount) AS INT) AS total_repaid,
  CAST(SUM(la.loan_amount) - SUM(lts.amount) AS INT) AS current_balance,
  CAST(AVG(DISTINCT la.interest_rate) AS FLOAT) AS average_interest_rate
FROM loan_transactions_silver lts
INNER JOIN loan_agreements_silver la
  ON lts.loan_id = la.loan_id
GROUP BY
  lts.branch_id,
  lts.customer_id
"""

loan_analytics_silver_df = spark.sql(loan_analytics_silver_sql)

loan_analytics_silver_output_path = f"{TARGET_PATH}/loan_analytics_silver.csv"
(
    loan_analytics_silver_df.write.mode(WRITE_MODE)
    .option("header", "true")
    .csv(loan_analytics_silver_output_path)
)

job.commit()
