from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {
    'tables': [
        {'target_schema': 'bronze', 'target_table': 'sales_event_bronze', 'target_alias': 'seb', 'mapping_details': 'POS.sales_event se', 'description': 'Raw POS sales events mapped 1:1 from source sales_event payload. Includes transactional attributes such as transaction_id, order_id, store_id, terminal_id, cashier_id, product_id, product_name, category, sub_category, quantity, unit_price, discount, total_amount, payment_id, event_action.'},
        {'target_schema': 'bronze', 'target_table': 'payment_event_bronze', 'target_alias': 'peb', 'mapping_details': 'PAYMENT_GATEWAY.payment_event pe', 'description': 'Raw payment gateway events mapped 1:1 from source payment_event payload. Includes payment_id, transaction_id, payment_mode, provider, amount, currency, payment_status.'},
        {'target_schema': 'bronze', 'target_table': 'inventory_event_bronze', 'target_alias': 'ieb', 'mapping_details': 'INVENTORY_SYSTEM.inventory_event ie', 'description': 'Raw inventory system events mapped 1:1 from source inventory_event payload. Includes inventory_event_id, product_id, store_id, warehouse_id, change_type, quantity_changed, current_stock.'},
        {'target_schema': 'bronze', 'target_table': 'footfall_event_bronze', 'target_alias': 'feb', 'mapping_details': 'SENSOR.footfall_event fe', 'description': 'Raw sensor footfall events mapped 1:1 from source footfall_event payload. Includes footfall_event_id, store_id, entry_count, exit_count, sensor_id.'},
        {'target_schema': 'bronze', 'target_table': 'event_metadata_bronze', 'target_alias': 'emb', 'mapping_details': 'ALL_SOURCES.event_metadata em', 'description': 'Raw event envelope metadata mapped 1:1 from source event_metadata across all event types. Includes event_id, event_type, source_system, event_timestamp, ingestion_timestamp, batch_id, is_deleted.'}
    ],
    'columns': [
        {'source_column': "['seb.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'seb.transaction_id = seb.transaction_id', 'target_table': 'seb'},
        {'source_column': "['seb.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'seb.store_id = seb.store_id', 'target_table': 'seb'},
        {'source_column': "['seb.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'seb.product_id = seb.product_id', 'target_table': 'seb'},
        {'source_column': "['emb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_null', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_null', 'transformation': 'emb.event_timestamp = emb.event_timestamp', 'target_table': 'emb'},
        {'source_column': "['seb.quantity']", 'source_type': 'INT', 'source_nullable': 'not_null', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_null', 'transformation': 'seb.quantity = seb.quantity', 'target_table': 'seb'},
        {'source_column': "['seb.total_amount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'not_null', 'target_column': 'total_amount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'not_null', 'transformation': 'seb.total_amount = seb.total_amount', 'target_table': 'seb'},
        {'source_column': "['seb.product_name']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'seb.product_name = seb.product_name', 'target_table': 'seb'},
        {'source_column': "['seb.category']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'category', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'seb.category = seb.category', 'target_table': 'seb'}
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
    mapping_details = table['mapping_details'].split(' ')
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    df = spark.read.format(read_format).option("header", "true").option("inferSchema", "true").load(base_path + source_table + '.' + read_format)
    df = df.alias(source_alias)
    
    transformations = []
    for column in metadata['columns']:
        if column['target_table'] == target_alias:
            rhs = column['transformation'].split('=')[1].strip()
            target_column = column['target_column']
            transformations.append(f"{rhs} as {target_column}")
    
    df = df.selectExpr(*transformations)
    df.write.mode(write_mode).format(write_format).option("header", "true").save(target_path + target_table + '.' + write_format)

job.commit()
