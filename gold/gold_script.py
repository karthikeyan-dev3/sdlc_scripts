from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# Step 1 — Read source table(s)
df_loan_transactions_silver = (
    spark.read
        .format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/loan_transactions_silver.{FILE_FORMAT}")
)

df_customer_silver = (
    spark.read
        .format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/customer_silver.{FILE_FORMAT}")
)

df_branch_silver = (
    spark.read
        .format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/branch_silver.{FILE_FORMAT}")
)

df_loan_silver = (
    spark.read
        .format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/loan_silver.{FILE_FORMAT}")
)

df_daily_refresh_log_silver = (
    spark.read
        .format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/daily_refresh_log_silver.{FILE_FORMAT}")
)

# Step 2 — Register temp view(s)
df_loan_transactions_silver.createOrReplaceTempView("loan_transactions_silver")
df_customer_silver.createOrReplaceTempView("customer_silver")
df_branch_silver.createOrReplaceTempView("branch_silver")
df_loan_silver.createOrReplaceTempView("loan_silver")
df_daily_refresh_log_silver.createOrReplaceTempView("daily_refresh_log_silver")

# ------------------------------------------------------------
# Target: gold_loan_transactions
# Step 3 — Write SQL transformation query
sql_gold_loan_transactions = """
SELECT
    CAST(lts.loan_id AS STRING) AS loan_id,
    CAST(lts.transaction_date AS DATE) AS transaction_date,
    CAST(lts.transaction_amount AS DECIMAL(18,2)) AS transaction_amount,
    CAST(lts.transaction_type AS STRING) AS transaction_type,
    CAST(lts.loan_balance AS DECIMAL(18,2)) AS loan_balance
FROM loan_transactions_silver lts
"""

# Step 4 — Execute query into a result DataFrame
df_gold_loan_transactions = spark.sql(sql_gold_loan_transactions)

# Step 5 — Write result as single CSV to target path
df_gold_loan_transactions.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/gold_loan_transactions.csv")

# ------------------------------------------------------------
# Target: gold_customer_data
# Step 3 — Write SQL transformation query
sql_gold_customer_data = """
SELECT
    CAST(cs.customer_id AS STRING) AS customer_id,
    CAST(cs.customer_name AS STRING) AS customer_name,
    CAST(cs.customer_segment AS STRING) AS customer_segment,
    CAST(cs.customer_credit_score AS INT) AS customer_credit_score,
    CAST(cs.customer_address AS STRING) AS customer_address
FROM customer_silver cs
"""

# Step 4 — Execute query into a result DataFrame
df_gold_customer_data = spark.sql(sql_gold_customer_data)

# Step 5 — Write result as single CSV to target path
df_gold_customer_data.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/gold_customer_data.csv")

# ------------------------------------------------------------
# Target: gold_branch_data
# Step 3 — Write SQL transformation query
sql_gold_branch_data = """
SELECT
    CAST(bs.branch_id AS STRING) AS branch_id,
    CAST(bs.branch_name AS STRING) AS branch_name,
    CAST(bs.branch_location AS STRING) AS branch_location,
    CAST(bs.branch_region AS STRING) AS branch_region
FROM branch_silver bs
"""

# Step 4 — Execute query into a result DataFrame
df_gold_branch_data = spark.sql(sql_gold_branch_data)

# Step 5 — Write result as single CSV to target path
df_gold_branch_data.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/gold_branch_data.csv")

# ------------------------------------------------------------
# Target: gold_aggregated_loan_data
# Step 3 — Write SQL transformation query
sql_gold_aggregated_loan_data = """
SELECT
    CAST(ls.loan_id AS STRING) AS loan_id,
    CAST(ls.customer_id AS STRING) AS customer_id,
    CAST(ls.branch_id AS STRING) AS branch_id,
    CAST(ls.total_loan_amount_source AS INT) AS total_loan_amount,
    CAST(SUM(CASE WHEN lts.transaction_type = 'repayment' THEN lts.transaction_amount ELSE 0 END) AS DECIMAL(18,2)) AS total_repayment_amount,
    CAST(MAX(lts.loan_balance) AS DECIMAL(18,2)) AS outstanding_balance,
    CAST(AVG(lts.transaction_amount) AS DECIMAL(18,2)) AS average_transaction_amount,
    CAST(ls.loan_status AS STRING) AS loan_status
FROM loan_silver ls
LEFT JOIN loan_transactions_silver lts
    ON ls.loan_id = lts.loan_id
GROUP BY
    ls.loan_id,
    ls.customer_id,
    ls.branch_id,
    ls.total_loan_amount_source,
    ls.loan_status
"""

# Step 4 — Execute query into a result DataFrame
df_gold_aggregated_loan_data = spark.sql(sql_gold_aggregated_loan_data)

# Step 5 — Write result as single CSV to target path
df_gold_aggregated_loan_data.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/gold_aggregated_loan_data.csv")

# ------------------------------------------------------------
# Target: gold_daily_refresh_log
# Step 3 — Write SQL transformation query
sql_gold_daily_refresh_log = """
SELECT
    CAST(drls.refresh_date AS DATE) AS refresh_date,
    CAST(drls.status AS STRING) AS status,
    CAST(drls.refreshed_records_count AS INT) AS refreshed_records_count
FROM daily_refresh_log_silver drls
"""

# Step 4 — Execute query into a result DataFrame
df_gold_daily_refresh_log = spark.sql(sql_gold_daily_refresh_log)

# Step 5 — Write result as single CSV to target path
df_gold_daily_refresh_log.coalesce(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save(f"{TARGET_PATH}/gold_daily_refresh_log.csv")

job.commit()