from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("silver_gold_job", {})

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"
WRITE_MODE = "overwrite"

# ============================================================
# Source Reads (Silver) + Temp Views
# ============================================================

customers_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/customers_silver.{FILE_FORMAT}/")
)
customers_silver_df.createOrReplaceTempView("customers_silver")

loan_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_transactions_silver.{FILE_FORMAT}/")
)
loan_transactions_silver_df.createOrReplaceTempView("loan_transactions_silver")

loan_performance_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/loan_performance_daily_silver.{FILE_FORMAT}/")
)
loan_performance_daily_silver_df.createOrReplaceTempView("loan_performance_daily_silver")

branches_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/branches_silver.{FILE_FORMAT}/")
)
branches_silver_df.createOrReplaceTempView("branches_silver")

# ============================================================
# Target: gold.gold_customers
# ============================================================

gold_customers_sql = """
SELECT
    cs.customer_id AS customer_id,
    cs.customer_name AS customer_name,
    cs.customer_segment AS customer_segment,
    CAST(cs.annual_income AS INT) AS annual_income,
    CAST(cs.credit_score AS INT) AS credit_score
FROM customers_silver cs
"""

gold_customers_df = spark.sql(gold_customers_sql)

gold_customers_output_path = TARGET_PATH + "/gold_customers.csv"
gold_customers_df.write.mode(WRITE_MODE).option("header", "true").csv(gold_customers_output_path)

# ============================================================
# Target: gold.gold_loan_transactions
# ============================================================

gold_loan_transactions_sql = """
SELECT
    lts.loan_id AS loan_id,
    CAST(lts.transaction_date AS DATE) AS transaction_date,
    CAST(lts.amount AS INT) AS amount,
    lts.repayment_status AS repayment_status,
    lts.customer_id AS customer_id,
    lts.branch_id AS branch_id,
    lts.loan_type AS loan_type,
    CAST(lts.outstanding_balance AS INT) AS outstanding_balance
FROM loan_transactions_silver lts
LEFT JOIN customers_silver cs
    ON lts.customer_id = cs.customer_id
LEFT JOIN branches_silver bs
    ON lts.branch_id = bs.branch_id
"""

gold_loan_transactions_df = spark.sql(gold_loan_transactions_sql)

gold_loan_transactions_output_path = TARGET_PATH + "/gold_loan_transactions.csv"
gold_loan_transactions_df.write.mode(WRITE_MODE).option("header", "true").csv(gold_loan_transactions_output_path)

# ============================================================
# Target: gold.gold_loan_performance_aggregated
# ============================================================

gold_loan_performance_aggregated_sql = """
SELECT
    CAST(lpds.date AS DATE) AS date,
    lpds.branch_id AS branch_id,
    CAST(lpds.total_loans_issued AS INT) AS total_loans_issued,
    CAST(lpds.total_repayments AS INT) AS total_repayments,
    CAST(lpds.average_loan_amount AS FLOAT) AS average_loan_amount,
    CAST(lpds.repayment_rate AS DOUBLE) AS repayment_rate,
    CAST(lpds.default_rate AS DOUBLE) AS default_rate
FROM loan_performance_daily_silver lpds
"""

gold_loan_performance_aggregated_df = spark.sql(gold_loan_performance_aggregated_sql)

gold_loan_performance_aggregated_output_path = TARGET_PATH + "/gold_loan_performance_aggregated.csv"
gold_loan_performance_aggregated_df.write.mode(WRITE_MODE).option("header", "true").csv(gold_loan_performance_aggregated_output_path)

# ============================================================
# Target: gold.gold_branches
# ============================================================

gold_branches_sql = """
SELECT
    bs.branch_id AS branch_id,
    bs.branch_name AS branch_name,
    bs.region AS region,
    CAST(SUM(CAST(lpds.total_loans_issued AS INT)) AS INT) AS total_loans,
    CAST(AVG(CAST(lpds.repayment_rate AS DOUBLE)) - AVG(CAST(lpds.default_rate AS DOUBLE)) AS DOUBLE) AS branch_performance_score
FROM branches_silver bs
LEFT JOIN loan_performance_daily_silver lpds
    ON bs.branch_id = lpds.branch_id
GROUP BY
    bs.branch_id,
    bs.branch_name,
    bs.region
"""

gold_branches_df = spark.sql(gold_branches_sql)

gold_branches_output_path = TARGET_PATH + "/gold_branches.csv"
gold_branches_df.write.mode(WRITE_MODE).option("header", "true").csv(gold_branches_output_path)

job.commit()