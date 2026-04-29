from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

# Metadata
metadata = {
    'tables': [
        {'target_schema': 'bronze', 'target_table': 'pos_sales_event_bronze', 'target_alias': 'pseb', 'mapping_details': "source_event_stream.event_metadata + source_event_stream.sales_event WHERE event_metadata.event_type = 'sales'", 'description': 'Raw POS sales events at event-granularity. Store all fields from event_metadata (event_id, event_type, source_system, event_timestamp, ingestion_timestamp, batch_id, is_deleted) along with sales_event fields (transaction_id, order_id, store_id, terminal_id, cashier_id, product_id, product_name, category, sub_category, quantity, unit_price, discount, total_amount, payment_id, event_action). No deduplication or normalization (e.g., category case) applied in bronze.'},
        {'target_schema': 'bronze', 'target_table': 'payment_gateway_payment_event_bronze', 'target_alias': 'pgpeb', 'mapping_details': "source_event_stream.event_metadata + source_event_stream.payment_event WHERE event_metadata.event_type = 'payment'", 'description': 'Raw payment gateway payment events at event-granularity. Store all fields from event_metadata along with payment_event fields (payment_id, transaction_id, payment_mode, provider, amount, currency, payment_status). Allows multiple payments per transaction and multiple currencies without transformation in bronze.'},
        {'target_schema': 'bronze', 'target_table': 'inventory_system_inventory_event_bronze', 'target_alias': 'isieb', 'mapping_details': "source_event_stream.event_metadata + source_event_stream.inventory_event WHERE event_metadata.event_type = 'inventory'", 'description': 'Raw inventory system inventory events at event-granularity. Store all fields from event_metadata along with inventory_event fields (inventory_event_id, product_id, store_id, warehouse_id, change_type, quantity_changed, current_stock). No reconciliation with sales/returns in bronze.'},
        {'target_schema': 'bronze', 'target_table': 'sensor_footfall_event_bronze', 'target_alias': 'sfeb', 'mapping_details': "source_event_stream.event_metadata + source_event_stream.footfall_event WHERE event_metadata.event_type = 'footfall'", 'description': 'Raw sensor footfall events at event-granularity. Store all fields from event_metadata along with footfall_event fields (footfall_event_id, store_id, entry_count, exit_count, sensor_id). No aggregations or time-windowing in bronze.'}
    ],
    'columns': [
        {'source_column': "['pseb.source_system']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.source_system = pseb.source_system', 'target_table': 'pseb'}, 
        {'source_column': "['pseb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'last_run_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.ingestion_timestamp = pseb.ingestion_timestamp', 'target_table': 'pseb'}
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    source_table = table['mapping_details'].split('+')[0].split('.')[1].strip()
    source_alias = table['target_alias']
    target_table = table['target_table']

    df = spark.read.format(read_format).option("header", "true").option("inferSchema", "true").load(f"{base_path}{source_table}.{read_format}")
    df = df.alias(source_alias)

    # Apply transformations
    transformations = [
        col['transformation'].split('= ')[1] + f" as {col['target_column']}"
        for col in metadata['columns'] if col['target_table'] == source_alias
    ]

    df = df.selectExpr(*transformations)
    
    df.write.mode(write_mode).format(write_format).option("header", "true").save(f"{target_path}{target_table}.{write_format}")

job.commit()