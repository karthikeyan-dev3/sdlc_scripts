from awsglue.context import GlueContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('GlueApp').getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Read sales_transactions_silver
sales_transactions_df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('s3://sdlc-agent-bucket/engineering-agent/silver/sales_transactions_silver.csv/')

# Read stores_silver
stores_df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('s3://sdlc-agent-bucket/engineering-agent/silver/stores_silver.csv/')

# Read products_silver
products_df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('s3://sdlc-agent-bucket/engineering-agent/silver/products_silver.csv/')

# Create temporary views
sales_transactions_df.createOrReplaceTempView('sales_transactions_silver')
stores_df.createOrReplaceTempView('stores_silver')
products_df.createOrReplaceTempView('products_silver')

# Transform and write gold_sales
gold_sales_df = spark.sql('''
    SELECT
        sts.transaction_id,
        sts.store_id,
        sts.product_id,
        CAST(sts.sale_date AS DATE) AS sale_date,
        CAST(sts.quantity_sold AS INT) AS quantity_sold,
        CAST(sts.total_revenue AS DOUBLE) AS total_revenue,
        UPPER(TRIM(sts.sales_channel)) AS sales_channel
    FROM sales_transactions_silver sts
    LEFT JOIN stores_silver ss ON sts.store_id = ss.store_id
    LEFT JOIN products_silver ps ON sts.product_id = ps.product_id
''')

gold_sales_df.write.csv('s3://sdlc-agent-bucket/engineering-agent/gold/gold_sales.csv', header=True, mode='overwrite')

# Read store_performance_silver
store_performance_df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('s3://sdlc-agent-bucket/engineering-agent/silver/store_performance_silver.csv/')

# Create temporary views
store_performance_df.createOrReplaceTempView('store_performance_silver')

# Transform and write gold_store_performance
gold_store_performance_df = spark.sql('''
    SELECT
        sps.store_id,
        CAST(sps.total_revenue AS DOUBLE) AS total_revenue,
        CAST(sps.average_daily_sales AS DOUBLE) AS average_daily_sales,
        TRIM(sps.location) AS location,
        CAST(sps.last_updated AS TIMESTAMP) AS last_updated
    FROM store_performance_silver sps
''')

gold_store_performance_df.write.csv('s3://sdlc-agent-bucket/engineering-agent/gold/gold_store_performance.csv', header=True, mode='overwrite')

# Read product_performance_silver
product_performance_df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('s3://sdlc-agent-bucket/engineering-agent/silver/product_performance_silver.csv/')

# Create temporary views
product_performance_df.createOrReplaceTempView('product_performance_silver')

# Transform and write gold_product_performance
gold_product_performance_df = spark.sql('''
    SELECT
        pps.product_id,
        UPPER(pps.category) AS category,
        CAST(pps.total_units_sold AS INT) AS total_units_sold,
        CAST(pps.total_revenue AS DOUBLE) AS total_revenue,
        CAST(pps.top_products_flag AS BOOLEAN) AS top_products_flag
    FROM product_performance_silver pps
''')

gold_product_performance_df.write.csv('s3://sdlc-agent-bucket/engineering-agent/gold/gold_product_performance.csv', header=True, mode='overwrite')

# Read data_quality_silver
data_quality_df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('s3://sdlc-agent-bucket/engineering-agent/silver/data_quality_silver.csv/')

# Create temporary views
data_quality_df.createOrReplaceTempView('data_quality_silver')

# Transform and write gold_data_quality
gold_data_quality_df = spark.sql('''
    SELECT
        dqs.record_id,
        CAST(dqs.data_validity_flag AS BOOLEAN) AS data_validity_flag,
        CAST(dqs.duplicates_flag AS BOOLEAN) AS duplicates_flag,
        CAST(dqs.last_validated AS TIMESTAMP) AS last_validated
    FROM data_quality_silver dqs
''')

gold_data_quality_df.write.csv('s3://sdlc-agent-bucket/engineering-agent/gold/gold_data_quality.csv', header=True, mode='overwrite')

# Read dashboard_refresh_silver
dashboard_refresh_df = spark.read.format('csv') \
    .option('header', 'true') \
    .load('s3://sdlc-agent-bucket/engineering-agent/silver/dashboard_refresh_silver.csv/')

# Create temporary views
dashboard_refresh_df.createOrReplaceTempView('dashboard_refresh_silver')

# Transform and write gold_dashboard_refresh
gold_dashboard_refresh_df = spark.sql('''
    SELECT
        CAST(drs.last_refresh_date AS DATE) AS last_refresh_date,
        LOWER(TRIM(drs.refresh_status)) AS refresh_status
    FROM dashboard_refresh_silver drs
''')

gold_dashboard_refresh_df.write.csv('s3://sdlc-agent-bucket/engineering-agent/gold/gold_dashboard_refresh.csv', header=True, mode='overwrite')