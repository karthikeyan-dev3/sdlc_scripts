import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# -------------------------------------------------------------------
# Source Reads (Bronze)
# -------------------------------------------------------------------
sales_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
)
sales_bronze_df.createOrReplaceTempView("sales_bronze")

product_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_bronze.{FILE_FORMAT}/")
)
product_master_bronze_df.createOrReplaceTempView("product_master_bronze")

store_master_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_bronze.{FILE_FORMAT}/")
)
store_master_bronze_df.createOrReplaceTempView("store_master_bronze")

# -------------------------------------------------------------------
# Target: silver.sales_silver
# -------------------------------------------------------------------
sales_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(sb.transaction_id) AS STRING) AS sale_id,
        CAST(TRIM(sb.product_id) AS STRING) AS product_id,
        CAST(TRIM(sb.store_id) AS STRING) AS store_id,
        CAST(sb.transaction_time AS DATE) AS sale_date,
        CAST(sb.quantity AS INT) AS quantity_sold,
        CAST(sb.sale_amount AS DOUBLE) AS sales_amount,
        ROW_NUMBER() OVER (
          PARTITION BY sb.transaction_id
          ORDER BY sb.transaction_time DESC
        ) AS rn
      FROM sales_bronze sb
    )
    SELECT
      sale_id,
      product_id,
      store_id,
      sale_date,
      quantity_sold,
      sales_amount
    FROM base
    WHERE rn = 1
      AND sale_id IS NOT NULL
      AND product_id IS NOT NULL
      AND store_id IS NOT NULL
      AND sale_date IS NOT NULL
      AND quantity_sold IS NOT NULL
      AND sales_amount IS NOT NULL
      AND quantity_sold > 0
      AND sales_amount >= 0
    """
)
sales_silver_df.createOrReplaceTempView("sales_silver")

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

# -------------------------------------------------------------------
# Target: silver.product_master_silver
# -------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(pmb.product_id) AS STRING) AS product_id,
        CAST(TRIM(pmb.product_name) AS STRING) AS product_name,
        CAST(TRIM(pmb.category) AS STRING) AS category,
        CAST(TRIM(pmb.brand) AS STRING) AS brand,
        CAST(pmb.price AS FLOAT) AS price,
        CAST(pmb.is_active AS BOOLEAN) AS is_active,
        ROW_NUMBER() OVER (
          PARTITION BY pmb.product_id
          ORDER BY pmb.product_id DESC
        ) AS rn
      FROM product_master_bronze pmb
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      price,
      is_active
    FROM base
    WHERE rn = 1
      AND is_active = true
      AND product_id IS NOT NULL
      AND product_name IS NOT NULL
      AND category IS NOT NULL
      AND brand IS NOT NULL
      AND price IS NOT NULL
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

# -------------------------------------------------------------------
# Target: silver.store_master_silver
# -------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(smb.store_id) AS STRING) AS store_id,
        CAST(TRIM(smb.store_name) AS STRING) AS store_name,
        CONCAT(CAST(TRIM(smb.city) AS STRING), ', ', CAST(TRIM(smb.state) AS STRING)) AS location,
        CASE
          WHEN CAST(TRIM(smb.state) AS STRING) IN ('ME','NH','VT','MA','RI','CT','NY','NJ','PA') THEN 'Northeast'
          WHEN CAST(TRIM(smb.state) AS STRING) IN ('OH','MI','IN','IL','WI','MN','IA','MO','ND','SD','NE','KS') THEN 'Midwest'
          WHEN CAST(TRIM(smb.state) AS STRING) IN ('DE','MD','DC','VA','WV','NC','SC','GA','FL','KY','TN','MS','AL','OK','TX','AR','LA') THEN 'South'
          WHEN CAST(TRIM(smb.state) AS STRING) IN ('MT','ID','WY','CO','NM','AZ','UT','NV','WA','OR','CA','AK','HI') THEN 'West'
          ELSE 'Unknown'
        END AS region,
        CAST(TRIM(smb.store_type) AS STRING) AS store_type,
        ROW_NUMBER() OVER (
          PARTITION BY smb.store_id
          ORDER BY smb.store_id DESC
        ) AS rn
      FROM store_master_bronze smb
    )
    SELECT
      store_id,
      store_name,
      location,
      region,
      store_type
    FROM base
    WHERE rn = 1
      AND store_id IS NOT NULL
      AND store_name IS NOT NULL
      AND location IS NOT NULL
      AND region IS NOT NULL
      AND store_type IS NOT NULL
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

# -------------------------------------------------------------------
# Target: silver.sales_aggregate_silver
# -------------------------------------------------------------------
sales_aggregate_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        ss.sale_date AS sale_date,
        ss.sales_amount AS sales_amount,
        ss.quantity_sold AS quantity_sold,
        sms.region AS region,
        pms.category AS category
      FROM sales_silver ss
      INNER JOIN product_master_silver pms
        ON ss.product_id = pms.product_id
      INNER JOIN store_master_silver sms
        ON ss.store_id = sms.store_id
    ),
    daily AS (
      SELECT
        sale_date,
        SUM(sales_amount) AS total_sales_amount,
        SUM(quantity_sold) AS total_quantity_sold,
        AVG(sales_amount) AS average_sale_amount
      FROM base
      GROUP BY sale_date
    ),
    by_region AS (
      SELECT
        sale_date,
        region,
        SUM(sales_amount) AS region_sales_amount
      FROM base
      GROUP BY sale_date, region
    ),
    by_category AS (
      SELECT
        sale_date,
        category,
        SUM(sales_amount) AS category_sales_amount
      FROM base
      GROUP BY sale_date, category
    ),
    region_json AS (
      SELECT
        sale_date,
        TO_JSON(
          MAP_FROM_ENTRIES(
            COLLECT_LIST(
              NAMED_STRUCT('key', region, 'value', region_sales_amount)
            )
          )
        ) AS total_sales_by_region
      FROM by_region
      GROUP BY sale_date
    ),
    category_json AS (
      SELECT
        sale_date,
        TO_JSON(
          MAP_FROM_ENTRIES(
            COLLECT_LIST(
              NAMED_STRUCT('key', category, 'value', category_sales_amount)
            )
          )
        ) AS total_sales_by_product_category
      FROM by_category
      GROUP BY sale_date
    )
    SELECT
      d.sale_date AS sale_date,
      d.total_sales_amount AS total_sales_amount,
      d.total_quantity_sold AS total_quantity_sold,
      d.average_sale_amount AS average_sale_amount,
      r.total_sales_by_region AS total_sales_by_region,
      c.total_sales_by_product_category AS total_sales_by_product_category
    FROM daily d
    INNER JOIN region_json r
      ON d.sale_date = r.sale_date
    INNER JOIN category_json c
      ON d.sale_date = c.sale_date
    """
)
sales_aggregate_silver_df.createOrReplaceTempView("sales_aggregate_silver")

(
    sales_aggregate_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregate_silver.csv")
)

# -------------------------------------------------------------------
# Target: silver.data_quality_metrics_silver
# -------------------------------------------------------------------
data_quality_metrics_silver_df = spark.sql(
    """
    SELECT
      CURRENT_DATE() AS execution_date,
      COUNT(ss.sale_id) AS num_records_processed
    FROM sales_silver ss
    """
)
data_quality_metrics_silver_df.createOrReplaceTempView("data_quality_metrics_silver")

(
    data_quality_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_metrics_silver.csv")
)

job.commit()