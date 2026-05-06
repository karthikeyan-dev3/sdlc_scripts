from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, trim, upper, coalesce, nullif, sum as spark_sum, countDistinct

# Initialize Spark and Glue context
spark = SparkSession.builder.appName("AWS Glue PySpark Job").getOrCreate()
glueContext = GlueContext(spark)

# Read sales_transactions_bronze into a DataFrame
sales_transactions_bronze_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/sales_transactions_bronze.csv/"))
sales_transactions_bronze_df.createOrReplaceTempView("stb")

# Transform and write sales_transactions_silver
sales_transactions_silver_df = spark.sql("""
SELECT 
    upper(trim(stb.transaction_id)) AS transaction_id,
    upper(trim(stb.store_id)) AS store_id,
    upper(trim(stb.product_id)) AS product_id,
    CAST(stb.transaction_time AS timestamp) AS transaction_time,
    CAST(stb.transaction_time AS date) AS transaction_date,
    stb.quantity AS quantity_sold,
    stb.sale_amount AS revenue
FROM 
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn FROM stb) stb
WHERE 
    rn = 1
    AND stb.quantity > 0
    AND stb.sale_amount >= 0
""")
sales_transactions_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv", header=True)

# Read stores_bronze into a DataFrame
stores_bronze_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/stores_bronze.csv/"))
stores_bronze_df.createOrReplaceTempView("sb")

# Transform and write store_information_silver
store_information_silver_df = spark.sql("""
SELECT 
    upper(trim(sb.store_id)) AS store_id,
    nullif(trim(sb.store_name), '') AS store_name,
    nullif(trim(sb.state), '') AS state,
    region_from_state(nullif(trim(sb.state), '')) AS region
FROM 
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY open_date DESC, store_name IS NOT NULL DESC) AS rn FROM sb) sb
WHERE 
    rn = 1
""")
store_information_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/store_information_silver.csv", header=True)

# Read products_bronze into a DataFrame
products_bronze_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .load("s3://sdlc-agent-bucket/engineering-agent/bronze/products_bronze.csv/"))
products_bronze_df.createOrReplaceTempView("pb")

# Transform and write product_details_silver
product_details_silver_df = spark.sql("""
SELECT 
    upper(trim(pb.product_id)) AS product_id,
    nullif(trim(pb.product_name), '') AS product_name,
    nullif(trim(pb.category), '') AS product_category
FROM 
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY is_active DESC, product_name IS NOT NULL DESC) AS rn FROM pb) pb
WHERE 
    rn = 1
""")
product_details_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/product_details_silver.csv", header=True)

# Join and aggregate for store_sales_aggregation_silver
store_sales_aggregation_silver_df = spark.sql("""
SELECT 
    sts.store_id,
    sis.store_name,
    sis.region,
    spark_sum(sts.revenue) AS total_revenue,
    countDistinct(sts.transaction_id) AS total_transactions,
    spark_sum(sts.revenue) / nullif(countDistinct(sts.transaction_id), 0) AS average_transaction_value,
    top_by_store(sts.product_id, sts.revenue, sts.quantity_sold) AS top_selling_product_id,
    product_name_for_top_by_store(pds.product_name, sts.product_id, sts.revenue, sts.quantity_sold) AS top_selling_product_name
FROM 
    sales_transactions_silver sts
INNER JOIN 
    store_information_silver sis ON sts.store_id = sis.store_id
LEFT JOIN 
    product_details_silver pds ON sts.product_id = pds.product_id
GROUP BY 
    sts.store_id, sis.store_name, sis.region
""")
store_sales_aggregation_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/store_sales_aggregation_silver.csv", header=True)

# Join and aggregate for product_sales_aggregation_silver
product_sales_aggregation_silver_df = spark.sql("""
SELECT 
    sts.product_id,
    pds.product_name,
    pds.product_category,
    spark_sum(sts.revenue) AS total_revenue,
    spark_sum(sts.quantity_sold) AS total_units_sold,
    spark_sum(sts.revenue) / nullif(spark_sum(sts.quantity_sold), 0) AS average_price,
    top_by_product(sts.store_id, sts.revenue, sts.quantity_sold) AS top_selling_store_id,
    store_name_for_top_by_product(sis.store_name, sts.store_id, sts.revenue, sts.quantity_sold) AS top_selling_store_name
FROM 
    sales_transactions_silver sts
INNER JOIN 
    product_details_silver pds ON sts.product_id = pds.product_id
LEFT JOIN 
    store_information_silver sis ON sts.store_id = sis.store_id
GROUP BY 
    sts.product_id, pds.product_name, pds.product_category
""")
product_sales_aggregation_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/product_sales_aggregation_silver.csv", header=True)