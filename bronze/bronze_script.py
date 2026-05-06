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
        {'target_schema': 'bronze', 'target_table': 'sales_event_bronze', 'target_alias': 'seb', 'mapping_details': 'POS.sales_event se'},
        {'target_schema': 'bronze', 'target_table': 'payment_event_bronze', 'target_alias': 'peb', 'mapping_details': 'PAYMENT_GATEWAY.payment_event pe'},
        {'target_schema': 'bronze', 'target_table': 'inventory_event_bronze', 'target_alias': 'ieb', 'mapping_details': 'INVENTORY_SYSTEM.inventory_event ie'},
        {'target_schema': 'bronze', 'target_table': 'footfall_event_bronze', 'target_alias': 'feb', 'mapping_details': 'SENSOR.footfall_event fe'},
        {'target_schema': 'bronze', 'target_table': 'event_metadata_bronze', 'target_alias': 'emb', 'mapping_details': 'ALL_SOURCES.event_metadata em'}
    ],
    'columns': [
        {'transformation': 'seb.transaction_id = seb.transaction_id', 'target_table': 'seb', 'target_column': 'transaction_id'},
        {'transformation': 'seb.order_id = seb.order_id', 'target_table': 'seb', 'target_column': 'order_id'},
        {'transformation': 'seb.store_id = seb.store_id', 'target_table': 'seb', 'target_column': 'store_id'},
        {'transformation': 'seb.terminal_id = seb.terminal_id', 'target_table': 'seb', 'target_column': 'terminal_id'},
        {'transformation': 'seb.cashier_id = seb.cashier_id', 'target_table': 'seb', 'target_column': 'cashier_id'},
        {'transformation': 'seb.product_id = seb.product_id', 'target_table': 'seb', 'target_column': 'product_id'},
        {'transformation': 'seb.product_name = seb.product_name', 'target_table': 'seb', 'target_column': 'product_name'},
        {'transformation': 'seb.category = seb.category', 'target_table': 'seb', 'target_column': 'category'},
        {'transformation': 'seb.sub_category = seb.sub_category', 'target_table': 'seb', 'target_column': 'sub_category'},
        {'transformation': 'seb.quantity = seb.quantity', 'target_table': 'seb', 'target_column': 'quantity'},
        {'transformation': 'seb.unit_price = seb.unit_price', 'target_table': 'seb', 'target_column': 'unit_price'},
        {'transformation': 'seb.discount = seb.discount', 'target_table': 'seb', 'target_column': 'discount'},
        {'transformation': 'seb.total_amount = seb.total_amount', 'target_table': 'seb', 'target_column': 'total_amount'},
        {'transformation': 'seb.payment_id = seb.payment_id', 'target_table': 'seb', 'target_column': 'payment_id'},
        {'transformation': 'seb.event_action = seb.event_action', 'target_table': 'seb', 'target_column': 'event_action'},
        {'transformation': 'peb.payment_id = peb.payment_id', 'target_table': 'peb', 'target_column': 'payment_id'},
        {'transformation': 'peb.transaction_id = peb.transaction_id', 'target_table': 'peb', 'target_column': 'transaction_id'},
        {'transformation': 'peb.payment_mode = peb.payment_mode', 'target_table': 'peb', 'target_column': 'payment_mode'},
        {'transformation': 'peb.provider = peb.provider', 'target_table': 'peb', 'target_column': 'provider'},
        {'transformation': 'peb.amount = peb.amount', 'target_table': 'peb', 'target_column': 'amount'},
        {'transformation': 'peb.currency = peb.currency', 'target_table': 'peb', 'target_column': 'currency'},
        {'transformation': 'peb.payment_status = peb.payment_status', 'target_table': 'peb', 'target_column': 'payment_status'},
        {'transformation': 'ieb.inventory_event_id = ieb.inventory_event_id', 'target_table': 'ieb', 'target_column': 'inventory_event_id'},
        {'transformation': 'ieb.product_id = ieb.product_id', 'target_table': 'ieb', 'target_column': 'product_id'},
        {'transformation': 'ieb.store_id = ieb.store_id', 'target_table': 'ieb', 'target_column': 'store_id'},
        {'transformation': 'ieb.warehouse_id = ieb.warehouse_id', 'target_table': 'ieb', 'target_column': 'warehouse_id'},
        {'transformation': 'ieb.change_type = ieb.change_type', 'target_table': 'ieb', 'target_column': 'change_type'},
        {'transformation': 'ieb.quantity_changed = ieb.quantity_changed', 'target_table': 'ieb', 'target_column': 'quantity_changed'},
        {'transformation': 'ieb.current_stock = ieb.current_stock', 'target_table': 'ieb', 'target_column': 'current_stock'},
        {'transformation': 'feb.footfall_event_id = feb.footfall_event_id', 'target_table': 'feb', 'target_column': 'footfall_event_id'},
        {'transformation': 'feb.store_id = feb.store_id', 'target_table': 'feb', 'target_column': 'store_id'},
        {'transformation': 'feb.entry_count = feb.entry_count', 'target_table': 'feb', 'target_column': 'entry_count'},
        {'transformation': 'feb.exit_count = feb.exit_count', 'target_table': 'feb', 'target_column': 'exit_count'},
        {'transformation': 'feb.sensor_id = feb.sensor_id', 'target_table': 'feb', 'target_column': 'sensor_id'},
        {'transformation': 'emb.event_id = emb.event_id', 'target_table': 'emb', 'target_column': 'event_id'},
        {'transformation': 'emb.event_type = emb.event_type', 'target_table': 'emb', 'target_column': 'event_type'},
        {'transformation': 'emb.source_system = emb.source_system', 'target_table': 'emb', 'target_column': 'source_system'},
        {'transformation': 'emb.event_timestamp = emb.event_timestamp', 'target_table': 'emb', 'target_column': 'event_timestamp'},
        {'transformation': 'emb.ingestion_timestamp = emb.ingestion_timestamp', 'target_table': 'emb', 'target_column': 'ingestion_timestamp'},
        {'transformation': 'emb.batch_id = emb.batch_id', 'target_table': 'emb', 'target_column': 'batch_id'},
        {'transformation': 'emb.is_deleted = emb.is_deleted', 'target_table': 'emb', 'target_column': 'is_deleted'}
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
    mapping_details = table['mapping_details']
    source_table, source_alias = mapping_details.split()
    target_table = table['target_table']
    target_alias = table['target_alias']

    df = spark.read.format(read_format)\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(base_path + source_table + '.' + read_format)

    df = df.alias(source_alias)

    transformations = [
        col['transformation'].split('=')[1].strip() + ' as ' + col['target_column']
        for col in metadata['columns'] if col['target_table'] == target_alias
    ]

    df = df.selectExpr(*transformations)

    df.write.mode(write_mode)\
        .format(write_format)\
        .option("header", "true")\
        .save(target_path + target_table + '.' + write_format)

job.commit()
