```python
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, coalesce, concat
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sum as _sum, avg, countDistinct

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

###################################
# Table 1: product_details_silver #
###################################

# Read source tables
product_details_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/product_details_bronze.{FILE_FORMAT}/")

# Create temp view
product_details_df.createOrReplaceTempView("pdb")

# Write a clear SQL query
product_details_silver_df = spark.sql("""
    SELECT product_id,
           TRIM(UPPER(product_name)) AS product_name,
           TRIM(UPPER(category)) AS category,
           TRIM(UPPER(brand)) AS brand,
           CAST(price AS DECIMAL(10, 2)) AS price
    FROM (
        SELECT product_id,
               product_name,
               category,
               brand,
               price,
               ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY update_timestamp DESC) AS rn
        FROM pdb
        WHERE is_active = 'true'
    ) t
    WHERE rn = 1 AND product_id IS NOT NULL AND product_name IS NOT NULL
""")

# Save output
product_details_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/product_details_silver.csv")

####################################
# Table 2: store_information_silver #
####################################

# Read source tables
store_information_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/store_information_bronze.{FILE_FORMAT}/")

# Create temp view
store_information_df.createOrReplaceTempView("sib")

# Write a clear SQL query
store_information_silver_df = spark.sql("""
    SELECT store_id,
           TRIM(UPPER(store_name)) AS store_name,
           CONCAT(TRIM(UPPER(city)), ', ', TRIM(UPPER(state))) AS location,
           TRIM(UPPER(state)) AS region,
           store_type
    FROM (
        SELECT store_id,
               store_name,
               city,
               state,
               store_type,
               ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY update_timestamp DESC) AS rn
        FROM sib
    ) t
    WHERE rn = 1 AND store_id IS NOT NULL AND store_name IS NOT NULL
""")

# Save output
store_information_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/store_information_silver.csv")

###################################
# Table 3: sales_transactions_silver #
###################################

# Read source tables
sales_transactions_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")

# Create temp view
sales_transactions_df.createOrReplaceTempView("stb")
product_details_silver_df.createOrReplaceTempView("pds")
store_information_silver_df.createOrReplaceTempView("sis")

# Write a clear SQL query
sales_transactions_silver_df = spark.sql("""
    SELECT stb.transaction_id,
           stb.product_id,
           stb.store_id,
           CAST(stb.transaction_time AS DATE) AS sales_date,
           stb.quantity AS quantity_sold,
           stb.sale_amount AS total_sales_amount
    FROM (
        SELECT stb.*, 
               ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
        FROM stb
        JOIN pds ON stb.product_id = pds.product_id
        JOIN sis ON stb.store_id = sis.store_id
        WHERE stb.quantity >= 0 AND stb.sale_amount >= 0
    ) t
    WHERE rn = 1
""")

# Save output
sales_transactions_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/sales_transactions_silver.csv")

##############################
# Table 4: aggregated_sales_silver #
##############################

# Create temp view
sales_transactions_silver_df.createOrReplaceTempView("sts")

# Write a clear SQL query
aggregated_sales_silver_df = spark.sql("""
    SELECT sts.sales_date,
           sis.region,
           SUM(sts.total_sales_amount) AS total_sales_amount,
           SUM(sts.quantity_sold) AS total_quantity_sold,
           SUM(sts.total_sales_amount) / COUNT(DISTINCT sts.store_id) AS average_sales_per_store
    FROM sts
    JOIN sis ON sts.store_id = sis.store_id
    GROUP BY sts.sales_date, sis.region
""")

# Save output
aggregated_sales_silver_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/aggregated_sales_silver.csv")
```