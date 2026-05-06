from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

# Metadata
runtime_config = {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}
tables = [
    {'target_schema': 'bronze', 'target_table': 'pos_sales_event_bronze', 'target_alias': 'pseb', 'mapping_details': "SOURCE: POS sales_event + event_metadata (event_type='sales') -> bronze.pos_sales_event_bronze"},
    {'target_schema': 'bronze', 'target_table': 'payment_gateway_event_bronze', 'target_alias': 'pgeb', 'mapping_details': "SOURCE: PAYMENT_GATEWAY payment_event + event_metadata (event_type='payment') -> bronze.payment_gateway_event_bronze"}
]
columns = [
    {'source_column': "['pseb.transaction_id']", 'transformation': 'pseb.transaction_id = pseb.transaction_id', 'target_table': 'pseb'},
    {'source_column': "['pseb.event_timestamp']", 'transformation': 'CAST(pseb.event_timestamp AS DATE) = transaction_date', 'target_table': 'CAST(pseb'},
    {'source_column': "['pseb.store_id']", 'transformation': 'pseb.store_id = pseb.store_id', 'target_table': 'pseb'},
    {'source_column': "['pseb.product_id']", 'transformation': 'pseb.product_id = pseb.product_id', 'target_table': 'pseb'},
    {'source_column': "['pseb.quantity']", 'transformation': 'pseb.quantity = quantity_sold', 'target_table': 'pseb'},
    {'source_column': "['pseb.total_amount']", 'transformation': 'pseb.total_amount = sales_amount', 'target_table': 'pseb'},
    {'source_column': "['pseb.product_name']", 'transformation': 'pseb.product_name = pseb.product_name', 'target_table': 'pseb'},
    {'source_column': "['pseb.category']", 'transformation': 'pseb.category = pseb.category', 'target_table': 'pseb'},
    {'source_column': "['pseb.unit_price']", 'transformation': 'pseb.unit_price = price', 'target_table': 'pseb'}
]

for table in tables:
    target_table = table['target_table']
    source_details = table['mapping_details'].split('SOURCE: ')[1].split(' ')[0]  # Extracting source table details
    source_table, source_alias = source_details.split(' ')[0], source_details.split(' ')[2]
    
    # Read data
    df = spark.read.format(runtime_config['read_format']) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(runtime_config['base_path'] + f"{source_table}." + runtime_config['read_format'])
    
    # Apply alias
    df = df.alias(source_alias)

    # Filter and Apply transformations
    transformations = []
    for column in columns:
        if column['target_table'] in target_table:
            transformation = column['transformation'].split('=')[1].strip()
            target_column = column['source_column'].strip("[]'")
            transformations.append(f"{transformation} as {target_column}")

    # Select
    df = df.selectExpr(*transformations)

    # Write Data
    df.write.mode(runtime_config['write_mode']) \
        .format(runtime_config['write_format']) \
        .option("header", "true") \
        .save(runtime_config['target_path'] + f"{target_table}." + runtime_config['write_format'])

job.commit()