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

# ----------------------------
# 1) Read source tables (Bronze) + temp views
# ----------------------------
stb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
stb_df.createOrReplaceTempView("stb")

pb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
pb_df.createOrReplaceTempView("pb")

sb_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
sb_df.createOrReplaceTempView("sb")

# ----------------------------
# 2) sales_transactions_silver (clean + dedup latest per transaction_id by transaction_time)
# ----------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH ranked AS (
        SELECT
            CAST(stb.transaction_id AS STRING) AS sale_id,
            CAST(stb.product_id AS STRING) AS product_id,
            CAST(stb.store_id AS STRING) AS store_id,
            DATE(CAST(stb.transaction_time AS TIMESTAMP)) AS sale_date,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sales_value,
            ROW_NUMBER() OVER (
                PARTITION BY stb.transaction_id
                ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
            ) AS rn
        FROM stb
    )
    SELECT
        sale_id,
        product_id,
        store_id,
        sale_date,
        quantity_sold,
        total_sales_value
    FROM ranked
    WHERE rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sts")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ----------------------------
# 3) product_silver (filter active + dedup by product_id keep latest record)
# ----------------------------
product_silver_df = spark.sql(
    """
    WITH filtered AS (
        SELECT
            CAST(pb.product_id AS STRING) AS product_id,
            CAST(pb.product_name AS STRING) AS product_name,
            CAST(pb.category AS STRING) AS product_category,
            CAST(pb.price AS FLOAT) AS product_price
        FROM pb
        WHERE CAST(pb.is_active AS BOOLEAN) = TRUE
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            product_category,
            product_price,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
        FROM filtered
    )
    SELECT
        product_id,
        product_name,
        product_category,
        product_price
    FROM ranked
    WHERE rn = 1
    """
)
product_silver_df.createOrReplaceTempView("ps")

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# ----------------------------
# 4) store_silver (build store_location + derive store_region from state + dedup by store_id)
# ----------------------------
store_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sb.store_id AS STRING) AS store_id,
            CAST(sb.store_name AS STRING) AS store_name,
            TRIM(COALESCE(CAST(sb.city AS STRING), '')) AS city,
            TRIM(COALESCE(CAST(sb.state AS STRING), '')) AS state
        FROM sb
    ),
    shaped AS (
        SELECT
            store_id,
            store_name,
            TRIM(
                CONCAT(
                    city,
                    CASE WHEN city <> '' AND state <> '' THEN ', ' ELSE '' END,
                    state
                )
            ) AS store_location,
            CASE
                WHEN UPPER(state) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
                WHEN UPPER(state) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
                WHEN UPPER(state) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
                WHEN UPPER(state) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
                ELSE 'UNKNOWN'
            END AS store_region
        FROM base
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            store_location,
            store_region,
            ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
        FROM shaped
    )
    SELECT
        store_id,
        store_name,
        store_location,
        store_region
    FROM ranked
    WHERE rn = 1
    """
)
store_silver_df.createOrReplaceTempView("ss")

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

# ----------------------------
# 5) sales_aggregated_silver (aggregate by sale_date, store_region, product_category)
# ----------------------------
sales_aggregated_silver_df = spark.sql(
    """
    SELECT
        sts.sale_date AS date,
        SUM(sts.total_sales_value) AS total_sales,
        SUM(sts.quantity_sold) AS total_quantity,
        SUM(sts.total_sales_value) / NULLIF(COUNT(DISTINCT sts.sale_id), 0) AS average_sale_value,
        ss.store_region AS region,
        ps.product_category AS product_category
    FROM sts
    INNER JOIN ss ON sts.store_id = ss.store_id
    INNER JOIN ps ON sts.product_id = ps.product_id
    GROUP BY
        sts.sale_date,
        ss.store_region,
        ps.product_category
    """
)

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()