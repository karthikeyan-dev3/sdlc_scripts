```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.getOrCreate()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read and transform sales_transaction_silver
df_pos_sales_event_bronze = spark.read.format(FILE_FORMAT).load(f"{SOURCE_PATH}/pos_sales_event_bronze.csv/")
df_pos_sales_event_bronze.createOrReplaceTempView("pseb")

sales_transaction_silver_df = spark.sql("""
SELECT
    TRIM(pseb.sales_event.transaction_id) AS transaction_id,
    TRIM(pseb.sales_event.product_id) AS product_id,
    TRIM(pseb.sales_event.store_id) AS store_id,
    CAST(pseb.event_metadata.event_timestamp AS TIMESTAMP) AS transaction_date,
    CAST(pseb.sales_event.total_amount AS DECIMAL(18,2)) AS sales_amount,
    CAST(pseb.sales_event.quantity AS INT) AS quantity_sold,
    TRIM(pseb.sales_event.product_name) AS product_name,
    TRIM(pseb.sales_event.category) AS product_category
FROM
    pseb
WHERE
    pseb.is_deleted = FALSE 
  AND ROW_NUMBER() OVER (PARTITION BY 
        pseb.sales_event.transaction_id, 
        pseb.sales_event.product_id, 
        pseb.sales_event.store_id 
      ORDER BY 
        pseb.event_metadata.event_timestamp DESC, 
        pseb.event_metadata.event_id DESC
      ) = 1
""")
sales_transaction_silver_df.write.option("header", "true").mode("overwrite").csv(f"{TARGET_PATH}/sales_transaction_silver.csv")

# Read and transform product_master_silver
sales_transaction_silver_df.createOrReplaceTempView("sts")

product_master_silver_df = spark.sql("""
SELECT
    sts.product_id,
    INITCAP(TRIM(sts.product_name)) AS product_name,
    INITCAP(TRIM(sts.product_category)) AS product_category,
    CAST(NULL AS STRING) AS manufacturer
FROM
    sts
WHERE
  ROW_NUMBER() OVER (PARTITION BY sts.product_id ORDER BY sts.transaction_date DESC) = 1
""")
product_master_silver_df.write.option("header", "true").mode("overwrite").csv(f"{TARGET_PATH}/product_master_silver.csv")

# Read and transform store_master_silver
store_master_silver_df = spark.sql("""
SELECT
    sts.store_id,
    CAST(NULL AS STRING) AS store_name,
    CAST(NULL AS STRING) AS store_location,
    CAST(NULL AS STRING) AS store_manager
FROM
    sts
WHERE
  ROW_NUMBER() OVER (PARTITION BY sts.store_id ORDER BY sts.transaction_date DESC) = 1
""")
store_master_silver_df.write.option("header", "true").mode("overwrite").csv(f"{TARGET_PATH}/store_master_silver.csv")

# Read and transform store_region_silver
store_region_silver_df = spark.sql("""
SELECT
    DISTINCT sts.store_id,
    CAST(NULL AS STRING) AS region
FROM
    sts
""")
store_region_silver_df.write.option("header", "true").mode("overwrite").csv(f"{TARGET_PATH}/store_region_silver.csv")

job.commit()
```