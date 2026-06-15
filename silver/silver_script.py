import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# =============================
# Source Reads (Bronze -> Temp Views)
# =============================

stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# Create databases (kept, although not required for temp/global temp views)
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# Use global temp views instead of multi-part temp view names (bronze.* is not valid for TEMP VIEW)
spark.sql(
    "CREATE OR REPLACE GLOBAL TEMP VIEW stores_bronze AS SELECT * FROM stores_bronze"
)
spark.sql(
    "CREATE OR REPLACE GLOBAL TEMP VIEW products_bronze AS SELECT * FROM products_bronze"
)
spark.sql(
    "CREATE OR REPLACE GLOBAL TEMP VIEW sales_transactions_bronze AS SELECT * FROM sales_transactions_bronze"
)

# =============================
# Target: silver.dim_store_silver
# =============================

dim_store_silver_df = spark.sql(
    """
    SELECT
      store_id,
      TRIM(store_name) AS store_name,
      TRIM(city) AS city,
      TRIM(state) AS state,
      TRIM(store_type) AS store_type,
      CAST(open_date AS date) AS open_date,
      CASE WHEN COALESCE(store_id,'') <> '' THEN TRUE ELSE FALSE END AS active_flag,
      CASE
        WHEN UPPER(TRIM(state)) IN ('CA','OR','WA','AK','HI') THEN 'WEST'
        WHEN UPPER(TRIM(state)) IN ('NY','NJ','PA','MA','CT','RI','VT','NH','ME') THEN 'NORTHEAST'
        WHEN UPPER(TRIM(state)) IN ('IL','IN','OH','MI','WI','MN','IA','MO','ND','SD','NE','KS') THEN 'MIDWEST'
        WHEN UPPER(TRIM(state)) IN ('TX','OK','AR','LA','MS','AL','GA','FL','SC','NC','TN','KY','VA','WV','MD','DE','DC') THEN 'SOUTH'
        ELSE 'UNKNOWN'
      END AS store_region
    FROM (
      SELECT
        sb.store_id,
        sb.store_name,
        sb.city,
        sb.state,
        sb.store_type,
        sb.open_date,
        ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.open_date DESC NULLS LAST) AS rn
      FROM global_temp.stores_bronze sb
      WHERE sb.store_id IS NOT NULL
    ) x
    WHERE x.rn = 1
    """
)

(
    dim_store_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_store_silver.csv")
)

# =============================
# Target: silver.dim_product_silver
# =============================

dim_product_silver_df = spark.sql(
    """
    SELECT
      product_id,
      TRIM(product_name) AS product_name,
      TRIM(brand) AS brand,
      TRIM(category) AS category,
      CAST(NULL AS string) AS subcategory,
      CAST(NULL AS string) AS unit_of_measure,
      COALESCE(is_active, TRUE) AS active_flag
    FROM (
      SELECT
        pb.product_id,
        pb.product_name,
        pb.brand,
        pb.category,
        pb.is_active,
        ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_name DESC NULLS LAST) AS rn
      FROM global_temp.products_bronze pb
      WHERE pb.product_id IS NOT NULL
    ) x
    WHERE x.rn = 1
    """
)

(
    dim_product_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_product_silver.csv")
)

# =============================
# Target: silver.sales_transactions_silver
# =============================

sales_transactions_silver_df = spark.sql(
    """
    SELECT
      transaction_id,
      store_id,
      product_id,
      CAST(quantity AS int) AS quantity,
      CAST(sale_amount AS double) AS sale_amount,
      CAST(transaction_time AS timestamp) AS transaction_time,
      CAST(transaction_time AS date) AS sales_date
    FROM (
      SELECT
        stb.transaction_id,
        stb.store_id,
        stb.product_id,
        stb.quantity,
        stb.sale_amount,
        stb.transaction_time,
        ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC NULLS LAST) AS rn
      FROM global_temp.sales_transactions_bronze stb
      WHERE stb.transaction_id IS NOT NULL
        AND stb.store_id IS NOT NULL
        AND stb.product_id IS NOT NULL
        AND stb.transaction_time IS NOT NULL
    ) x
    WHERE x.rn = 1
    """
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

job.commit()
