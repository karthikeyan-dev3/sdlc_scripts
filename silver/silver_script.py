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
# Read Source Tables from S3
# ----------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
sales_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
stores_bronze_df.createOrReplaceTempView("stores_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
sales_bronze_df.createOrReplaceTempView("sales_bronze")

# ============================================================
# Target: stores_silver
# ============================================================
stores_silver_df = spark.sql(
    """
    SELECT
        store_id,
        NULLIF(TRIM(store_name),'') AS store_name,
        NULLIF(TRIM(store_type),'') AS store_type,
        NULLIF(TRIM(city),'') AS city,
        NULLIF(TRIM(state),'') AS state,
        CAST(open_date AS date) AS open_date
    FROM (
        SELECT
            sb.store_id,
            sb.store_name,
            sb.store_type,
            sb.city,
            sb.state,
            sb.open_date,
            ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.open_date DESC) AS rn
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
    ) d
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

# ============================================================
# Target: products_silver
# ============================================================
products_silver_df = spark.sql(
    """
    SELECT
        product_id,
        NULLIF(TRIM(product_name),'') AS product_name,
        NULLIF(TRIM(category),'') AS category,
        NULLIF(TRIM(brand),'') AS brand,
        CAST(price AS double) AS price,
        COALESCE(CAST(is_active AS boolean), true) AS is_active
    FROM (
        SELECT
            pb.product_id,
            pb.product_name,
            pb.category,
            pb.brand,
            pb.price,
            pb.is_active,
            ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
    ) d
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

# ============================================================
# Target: sales_silver
# ============================================================
sales_silver_df = spark.sql(
    """
    SELECT
        transaction_id,
        store_id,
        product_id,
        CAST(quantity AS int) AS quantity,
        CAST(sale_amount AS double) AS sale_amount,
        CAST(transaction_time AS timestamp) AS transaction_time
    FROM (
        SELECT
            salb.transaction_id,
            NULLIF(TRIM(salb.store_id),'') AS store_id,
            NULLIF(TRIM(salb.product_id),'') AS product_id,
            salb.quantity,
            salb.sale_amount,
            salb.transaction_time,
            ROW_NUMBER() OVER (PARTITION BY salb.transaction_id ORDER BY salb.transaction_time DESC) AS rn
        FROM sales_bronze salb
        WHERE salb.transaction_id IS NOT NULL
    ) d
    WHERE rn = 1
      AND quantity IS NOT NULL
      AND quantity > 0
      AND sale_amount IS NOT NULL
    """
)

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

job.commit()