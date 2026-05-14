
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# --------------------------------------------------------------------------------------
# 1) Read source tables from S3
# --------------------------------------------------------------------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)

# --------------------------------------------------------------------------------------
# 2) Create temp views
# --------------------------------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# --------------------------------------------------------------------------------------
# 3) Transform + 4) Save output: sales_transactions_silver
# --------------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            stb.transaction_id AS transaction_id,
            CAST(stb.transaction_time AS date) AS transaction_date,
            stb.store_id AS store_id,
            stb.product_id AS product_id,
            CASE
                WHEN CAST(stb.quantity AS int) < 0 THEN 0
                ELSE CAST(stb.quantity AS int)
            END AS quantity,
            CASE
                WHEN CAST(stb.sale_amount AS double) < 0 THEN 0
                ELSE CAST(stb.sale_amount AS double)
            END AS revenue,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
    )
    SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity,
        revenue
    FROM ranked
    WHERE rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# --------------------------------------------------------------------------------------
# 3) Transform + 4) Save output: products_silver
# --------------------------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            pb.product_id AS product_id,
            pb.product_name AS product_name,
            pb.category AS category,
            ROW_NUMBER() OVER (
                PARTITION BY pb.product_id
                ORDER BY pb.product_id
            ) AS rn
        FROM products_bronze pb
    )
    SELECT
        product_id,
        product_name,
        category
    FROM ranked
    WHERE rn = 1
    """
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

products_silver_df.createOrReplaceTempView("products_silver")

# --------------------------------------------------------------------------------------
# 3) Transform + 4) Save output: stores_silver
# --------------------------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            sb.store_id AS store_id,
            sb.store_name AS store_name,
            sb.state AS region,
            ROW_NUMBER() OVER (
                PARTITION BY sb.store_id
                ORDER BY sb.store_id
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        region
    FROM ranked
    WHERE rn = 1
    """
)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

stores_silver_df.createOrReplaceTempView("stores_silver")

# --------------------------------------------------------------------------------------
# 3) Transform + 4) Save output: unified_sales_silver
# --------------------------------------------------------------------------------------
unified_sales_silver_df = spark.sql(
    """
    SELECT
        sts.transaction_id AS transaction_id,
        sts.transaction_date AS transaction_date,
        sts.store_id AS store_id,
        ss.store_name AS store_name,
        ss.region AS region,
        sts.product_id AS product_id,
        ps.product_name AS product_name,
        ps.category AS category,
        sts.quantity AS quantity,
        sts.revenue AS revenue
    FROM sales_transactions_silver sts
    INNER JOIN stores_silver ss
        ON sts.store_id = ss.store_id
    INNER JOIN products_silver ps
        ON sts.product_id = ps.product_id
    """
)

(
    unified_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/unified_sales_silver.csv")
)

job.commit()
