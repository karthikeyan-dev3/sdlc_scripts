import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# ----------------------------
# 1) Read source tables (Bronze) + Temp Views
# ----------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ----------------------------
# 2) product_master_silver (pms)
# ----------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(pb.product_id) AS product_id,
            TRIM(pb.product_name) AS product_name,
            TRIM(pb.category) AS category,
            TRIM(pb.brand) AS brand,
            CAST(pb.price AS FLOAT) AS price,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pb.product_id)
                ORDER BY
                    (CASE WHEN pb.product_name IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN pb.category IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN pb.brand IS NOT NULL AND TRIM(pb.brand) <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN pb.price IS NOT NULL AND TRIM(CAST(pb.price AS STRING)) <> '' THEN 1 ELSE 0 END) DESC
            ) AS rn
        FROM products_bronze pb
        WHERE pb.is_active = true
    )
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price
    FROM base
    WHERE rn = 1
    """
)
product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ----------------------------
# 3) store_master_silver (sms)
# ----------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            TRIM(sb.store_id) AS store_id,
            TRIM(sb.store_name) AS store_name,
            CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
            CASE
                WHEN UPPER(TRIM(sb.state)) IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
                WHEN UPPER(TRIM(sb.state)) IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
                WHEN UPPER(TRIM(sb.state)) IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
                WHEN UPPER(TRIM(sb.state)) IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
                ELSE NULL
            END AS region,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sb.store_id)
                ORDER BY
                    (CASE WHEN sb.store_name IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN sb.city IS NOT NULL AND TRIM(sb.city) <> '' THEN 1 ELSE 0 END) DESC,
                    (CASE WHEN sb.state IS NOT NULL AND TRIM(sb.state) <> '' THEN 1 ELSE 0 END) DESC
            ) AS rn
        FROM stores_bronze sb
    )
    SELECT
        store_id,
        store_name,
        location,
        region
    FROM base
    WHERE rn = 1
    """
)
store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ----------------------------
# 4) sales_transactions_silver (sts)
# ----------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH dedup AS (
        SELECT
            TRIM(stb.transaction_id) AS transaction_id,
            TRIM(stb.product_id) AS product_id,
            TRIM(stb.store_id) AS store_id,
            CAST(stb.transaction_time AS DATE) AS sale_date,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(stb.transaction_id)
                ORDER BY stb.transaction_time DESC
            ) AS rn
        FROM sales_transactions_bronze stb
    )
    SELECT
        d.transaction_id,
        d.product_id,
        d.store_id,
        d.sale_date,
        d.quantity_sold,
        d.total_sales_amount
    FROM dedup d
    INNER JOIN product_master_silver pms
        ON d.product_id = pms.product_id
    INNER JOIN store_master_silver sms
        ON d.store_id = sms.store_id
    WHERE d.rn = 1
      AND d.quantity_sold > 0
      AND d.total_sales_amount >= 0
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ----------------------------
# 5) aggregated_sales_silver (ass)
# ----------------------------
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
        sts.sale_date AS report_date,
        sts.product_id AS product_id,
        sts.store_id AS store_id,
        SUM(sts.quantity_sold) AS total_quantity_sold,
        SUM(sts.total_sales_amount) AS total_revenue
    FROM sales_transactions_silver sts
    GROUP BY
        sts.sale_date,
        sts.product_id,
        sts.store_id
    """
)
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()
