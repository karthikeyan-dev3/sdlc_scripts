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

# -----------------------------
# sales_silver
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

sales_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(stb.transaction_id AS STRING) AS transaction_id,
            CAST(stb.product_id AS STRING) AS product_id,
            CAST(stb.store_id AS STRING) AS store_id,
            CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS sales_date,
            CAST(stb.quantity AS INT) AS quantity,
            CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND stb.store_id IS NOT NULL
          AND stb.product_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            transaction_id,
            product_id,
            store_id,
            sales_date,
            quantity,
            sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY transaction_time DESC
            ) AS rn
        FROM base
    )
    SELECT
        transaction_id,
        product_id,
        store_id,
        sales_date,
        quantity,
        sales_amount
    FROM ranked
    WHERE rn = 1
    """
)

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# -----------------------------
# product_silver
# -----------------------------
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

product_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(pb.product_id AS STRING) AS product_id,
            CAST(pb.product_name AS STRING) AS product_name,
            CAST(pb.category AS STRING) AS category,
            CAST(pb.price AS FLOAT) AS price
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            product_id,
            product_name,
            category,
            price,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
                ORDER BY product_id
            ) AS rn
        FROM base
    )
    SELECT
        product_id,
        product_name,
        category,
        price
    FROM ranked
    WHERE rn = 1
    """
)

(
    product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_silver.csv")
)

# -----------------------------
# store_silver
# -----------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

store_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(sb.store_id AS STRING) AS store_id,
            CAST(sb.store_name AS STRING) AS store_name,
            CONCAT(CAST(sb.city AS STRING), ', ', CAST(sb.state AS STRING)) AS location,
            CASE
                WHEN sb.state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
                WHEN sb.state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
                WHEN sb.state IN ('DE','DC','FL','GA','MD','NC','SC','VA','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
                WHEN sb.state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
                ELSE 'Unknown'
            END AS region
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
    ),
    ranked AS (
        SELECT
            store_id,
            store_name,
            location,
            region,
            ROW_NUMBER() OVER (
                PARTITION BY store_id
                ORDER BY store_id
            ) AS rn
        FROM base
    )
    SELECT
        store_id,
        store_name,
        location,
        region
    FROM ranked
    WHERE rn = 1
    """
)

(
    store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_silver.csv")
)

job.commit()