import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
    TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
    FILE_FORMAT = "csv"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # ============================================================
    # 1) Read source tables from S3 (Bronze)
    # ============================================================

    sales_transactions_bronze_df = (
        spark.read.format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
    )
    sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

    product_bronze_df = (
        spark.read.format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{SOURCE_PATH}/product_bronze.{FILE_FORMAT}/")
    )
    product_bronze_df.createOrReplaceTempView("product_bronze")

    store_bronze_df = (
        spark.read.format(FILE_FORMAT)
        .option("header", "true")
        .option("inferSchema", "false")
        .load(f"{SOURCE_PATH}/store_bronze.{FILE_FORMAT}/")
    )
    store_bronze_df.createOrReplaceTempView("store_bronze")

    # ============================================================
    # 2) sales_transactions_silver
    # ============================================================

    sales_transactions_silver_sql = """
    SELECT
      transaction_id,
      store_id,
      product_id,
      quantity,
      sale_amount,
      transaction_time
    FROM (
      SELECT
        stb.transaction_id,
        stb.store_id,
        stb.product_id,
        CAST(stb.quantity AS INT) AS quantity,
        CAST(stb.sale_amount AS DOUBLE) AS sale_amount,
        CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
        ROW_NUMBER() OVER (
          PARTITION BY stb.transaction_id
          ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      WHERE stb.transaction_id IS NOT NULL
        AND stb.store_id IS NOT NULL
        AND stb.product_id IS NOT NULL
        AND stb.quantity IS NOT NULL
        AND stb.sale_amount IS NOT NULL
        AND stb.transaction_time IS NOT NULL
        AND CAST(stb.quantity AS INT) > 0
        AND CAST(stb.sale_amount AS DOUBLE) >= 0
    ) d
    WHERE rn = 1
    """

    sales_transactions_silver_df = spark.sql(sales_transactions_silver_sql)

    (
        sales_transactions_silver_df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
    )

    # ============================================================
    # 3) product_silver
    # ============================================================

    product_silver_sql = """
    SELECT
      product_id,
      product_name,
      category,
      brand,
      price,
      is_active
    FROM (
      SELECT
        pb.product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        CAST(pb.price AS FLOAT) AS price,
        CAST(pb.is_active AS BOOLEAN) AS is_active,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.product_id
        ) AS rn
      FROM product_bronze pb
      WHERE pb.product_id IS NOT NULL
        AND pb.product_name IS NOT NULL
        AND pb.category IS NOT NULL
        AND pb.brand IS NOT NULL
        AND pb.price IS NOT NULL
        AND pb.is_active IS NOT NULL
    ) d
    WHERE rn = 1
    """

    product_silver_df = spark.sql(product_silver_sql)

    (
        product_silver_df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/product_silver.csv")
    )

    # ============================================================
    # 4) store_silver
    # ============================================================

    store_silver_sql = """
    SELECT
      store_id,
      store_name,
      city,
      state,
      store_type,
      open_date
    FROM (
      SELECT
        sb.store_id,
        TRIM(sb.store_name) AS store_name,
        TRIM(sb.city) AS city,
        TRIM(sb.state) AS state,
        TRIM(sb.store_type) AS store_type,
        CAST(sb.open_date AS DATE) AS open_date,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.store_id
        ) AS rn
      FROM store_bronze sb
      WHERE sb.store_id IS NOT NULL
        AND sb.store_name IS NOT NULL
        AND sb.city IS NOT NULL
        AND sb.state IS NOT NULL
        AND sb.store_type IS NOT NULL
        AND sb.open_date IS NOT NULL
    ) d
    WHERE rn = 1
    """

    store_silver_df = spark.sql(store_silver_sql)

    (
        store_silver_df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .save(f"{TARGET_PATH}/store_silver.csv")
    )

    job.commit()


if __name__ == "__main__":
    main()