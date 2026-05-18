import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------
# Read Source Tables (S3) + Temp Views
# -------------------------------------------------------------------
stb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stb_df.createOrReplaceTempView("sales_transactions_bronze")

pb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
pb_df.createOrReplaceTempView("products_bronze")

sb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
sb_df.createOrReplaceTempView("stores_bronze")

# -------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            UPPER(TRIM(stb.transaction_id)) AS transaction_id,
            UPPER(TRIM(stb.product_id))     AS product_id,
            UPPER(TRIM(stb.store_id))       AS store_id,
            CAST(stb.transaction_time AS date) AS sale_date,
            CAST(GREATEST(COALESCE(CAST(stb.sale_amount AS double), 0D), 0D) AS decimal(38,10)) AS revenue,
            CAST(GREATEST(COALESCE(CAST(stb.quantity AS int), 0), 0) AS int) AS quantity_sold,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(stb.transaction_id))
                ORDER BY CAST(stb.transaction_time AS timestamp) DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND stb.product_id IS NOT NULL
          AND stb.store_id IS NOT NULL
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        sale_date,
        revenue,
        quantity_sold
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

# -------------------------------------------------------------------
# Target: silver.products_silver
# -------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            UPPER(TRIM(pb.product_id)) AS product_id,
            TRIM(pb.product_name)      AS product_name,
            UPPER(TRIM(pb.category))   AS category,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(pb.product_id))
                ORDER BY CASE WHEN LOWER(TRIM(pb.is_active)) = 'true' THEN 0 ELSE 1 END
            ) AS rn
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
          AND LOWER(TRIM(pb.is_active)) = 'true'
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

# -------------------------------------------------------------------
# Target: silver.stores_silver
# -------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            UPPER(TRIM(sb.store_id)) AS store_id,
            TRIM(sb.store_name)      AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(sb.store_id))
                ORDER BY
                    (CASE WHEN sb.store_name IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 0 ELSE 1 END) +
                    (CASE WHEN sb.city IS NOT NULL AND TRIM(sb.city) <> '' THEN 0 ELSE 1 END) +
                    (CASE WHEN sb.state IS NOT NULL AND TRIM(sb.state) <> '' THEN 0 ELSE 1 END)
            ) AS rn
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
    )
    SELECT
        store_id,
        store_name,
        location
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

job.commit()
