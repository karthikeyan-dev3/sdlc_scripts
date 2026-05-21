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
        {
            'target_schema': 'bronze',
            'target_table': 'pos_sales_event_bronze',
            'target_alias': 'pseb',
            'mapping_details': "SOURCE: POS sales events (records where event_metadata.event_type = 'sales'); MAP: event_metadata.* + sales_event.* as columns; NO JOINS/AGGS",
            'description': 'Bronze raw ingestion table for POS sales/return events, capturing both event metadata (event_id, timestamps, batch_id, is_deleted, source_system) and sales payload (transaction_id, order_id, store/terminal/cashier, product attributes, quantity, pricing, discount, total_amount, payment_id, event_action). Keeps duplicates/late arrivals as-is for downstream processing.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'payment_gateway_payment_event_bronze',
            'target_alias': 'pgpeb',
            'mapping_details': "SOURCE: PAYMENT_GATEWAY payment events (records where event_metadata.event_type = 'payment'); MAP: event_metadata.* + payment_event.* as columns; NO JOINS/AGGS",
            'description': 'Bronze raw ingestion table for payment events from the payment gateway, storing metadata and payment payload (payment_id, transaction_id, payment_mode, provider, amount, currency, payment_status). Supports multiple payments per transaction and mixed currencies without transformation.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'inventory_system_inventory_event_bronze',
            'target_alias': 'isieb',
            'mapping_details': "SOURCE: INVENTORY_SYSTEM inventory events (records where event_metadata.event_type = 'inventory'); MAP: event_metadata.* + inventory_event.* as columns; NO JOINS/AGGS",
            'description': 'Bronze raw ingestion table for inventory change events, storing metadata and inventory payload (inventory_event_id, product_id, store_id, warehouse_id, change_type, quantity_changed, current_stock).'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sensor_footfall_event_bronze',
            'target_alias': 'sfeb',
            'mapping_details': "SOURCE: SENSOR footfall events (records where event_metadata.event_type = 'footfall'); MAP: event_metadata.* + footfall_event.* as columns; NO JOINS/AGGS",
            'description': 'Bronze raw ingestion table for footfall sensor events, storing metadata and footfall payload (footfall_event_id, store_id, entry_count, exit_count, sensor_id).'
        }
    ],
    'columns': [
        {'source_column': "['pseb.event_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.event_id = pseb.event_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.event_type']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_type', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.event_type = pseb.event_type', 'target_table': 'pseb'},
        {'source_column': "['pseb.source_system']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.source_system = pseb.source_system', 'target_table': 'pseb'},
        {'source_column': "['pseb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.event_timestamp = pseb.event_timestamp', 'target_table': 'pseb'},
        {'source_column': "['pseb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'ingestion_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.ingestion_timestamp = pseb.ingestion_timestamp', 'target_table': 'pseb'},
        {'source_column': "['pseb.batch_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'batch_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.batch_id = pseb.batch_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.is_deleted']", 'source_type': 'BOOLEAN', 'source_nullable': 'NOT NULL', 'target_column': 'is_deleted', 'target_type': 'BOOLEAN', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.is_deleted = pseb.is_deleted', 'target_table': 'pseb'},
        {'source_column': "['pseb.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.transaction_id = pseb.transaction_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.order_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.order_id = pseb.order_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.store_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.store_id = pseb.store_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.terminal_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'terminal_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.terminal_id = pseb.terminal_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.cashier_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'cashier_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.cashier_id = pseb.cashier_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.product_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.product_id = pseb.product_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.product_name']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.product_name = pseb.product_name', 'target_table': 'pseb'},
        {'source_column': "['pseb.category']", 'source_type': 'STRING', 'source_nullable': 'nan', 'target_column': 'category', 'target_type': 'STRING', 'target_nullable': 'nan', 'transformation': 'pseb.category = pseb.category', 'target_table': 'pseb'},
        {'source_column': "['pseb.sub_category']", 'source_type': 'STRING', 'source_nullable': 'nan', 'target_column': 'sub_category', 'target_type': 'STRING', 'target_nullable': 'nan', 'transformation': 'pseb.sub_category = pseb.sub_category', 'target_table': 'pseb'},
        {'source_column': "['pseb.quantity']", 'source_type': 'INT', 'source_nullable': 'NOT NULL', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.quantity = pseb.quantity', 'target_table': 'pseb'},
        {'source_column': "['pseb.unit_price']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'NOT NULL', 'target_column': 'unit_price', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.unit_price = pseb.unit_price', 'target_table': 'pseb'},
        {'source_column': "['pseb.discount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'nan', 'target_column': 'discount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'nan', 'transformation': 'pseb.discount = pseb.discount', 'target_table': 'pseb'},
        {'source_column': "['pseb.total_amount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'NOT NULL', 'target_column': 'total_amount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.total_amount = pseb.total_amount', 'target_table': 'pseb'},
        {'source_column': "['pseb.payment_id']", 'source_type': 'STRING', 'source_nullable': 'nan', 'target_column': 'payment_id', 'target_type': 'STRING', 'target_nullable': 'nan', 'transformation': 'pseb.payment_id = pseb.payment_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.event_action']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_action', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pseb.event_action = pseb.event_action', 'target_table': 'pseb'},
        {'source_column': "['pgpeb.event_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.event_id = pgpeb.event_id', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.event_type']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_type', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.event_type = pgpeb.event_type', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.source_system']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.source_system = pgpeb.source_system', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.event_timestamp = pgpeb.event_timestamp', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'ingestion_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.ingestion_timestamp = pgpeb.ingestion_timestamp', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.batch_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'batch_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.batch_id = pgpeb.batch_id', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.is_deleted']", 'source_type': 'BOOLEAN', 'source_nullable': 'NOT NULL', 'target_column': 'is_deleted', 'target_type': 'BOOLEAN', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.is_deleted = pgpeb.is_deleted', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.payment_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'payment_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.payment_id = pgpeb.payment_id', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.transaction_id = pgpeb.transaction_id', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.payment_mode']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'payment_mode', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.payment_mode = pgpeb.payment_mode', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.provider']", 'source_type': 'STRING', 'source_nullable': 'nan', 'target_column': 'provider', 'target_type': 'STRING', 'target_nullable': 'nan', 'transformation': 'pgpeb.provider = pgpeb.provider', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.amount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'NOT NULL', 'target_column': 'amount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.amount = pgpeb.amount', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.currency']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'currency', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.currency = pgpeb.currency', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.payment_status']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'payment_status', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'pgpeb.payment_status = pgpeb.payment_status', 'target_table': 'pgpeb'},
        {'source_column': "['isieb.event_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.event_id = isieb.event_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.event_type']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_type', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.event_type = isieb.event_type', 'target_table': 'isieb'},
        {'source_column': "['isieb.source_system']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.source_system = isieb.source_system', 'target_table': 'isieb'},
        {'source_column': "['isieb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.event_timestamp = isieb.event_timestamp', 'target_table': 'isieb'},
        {'source_column': "['isieb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'ingestion_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.ingestion_timestamp = isieb.ingestion_timestamp', 'target_table': 'isieb'},
        {'source_column': "['isieb.batch_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'batch_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.batch_id = isieb.batch_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.is_deleted']", 'source_type': 'BOOLEAN', 'source_nullable': 'NOT NULL', 'target_column': 'is_deleted', 'target_type': 'BOOLEAN', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.is_deleted = isieb.is_deleted', 'target_table': 'isieb'},
        {'source_column': "['isieb.inventory_event_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'inventory_event_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.inventory_event_id = isieb.inventory_event_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.product_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.product_id = isieb.product_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.store_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.store_id = isieb.store_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.warehouse_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'warehouse_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.warehouse_id = isieb.warehouse_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.change_type']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'change_type', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.change_type = isieb.change_type', 'target_table': 'isieb'},
        {'source_column': "['isieb.quantity_changed']", 'source_type': 'INT', 'source_nullable': 'NOT NULL', 'target_column': 'quantity_changed', 'target_type': 'INT', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.quantity_changed = isieb.quantity_changed', 'target_table': 'isieb'},
        {'source_column': "['isieb.current_stock']", 'source_type': 'INT', 'source_nullable': 'NOT NULL', 'target_column': 'current_stock', 'target_type': 'INT', 'target_nullable': 'NOT NULL', 'transformation': 'isieb.current_stock = isieb.current_stock', 'target_table': 'isieb'},
        {'source_column': "['sfeb.event_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.event_id = sfeb.event_id', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.event_type']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'event_type', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.event_type = sfeb.event_type', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.source_system']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.source_system = sfeb.source_system', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.event_timestamp = sfeb.event_timestamp', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'NOT NULL', 'target_column': 'ingestion_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.ingestion_timestamp = sfeb.ingestion_timestamp', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.batch_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'batch_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.batch_id = sfeb.batch_id', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.is_deleted']", 'source_type': 'BOOLEAN', 'source_nullable': 'NOT NULL', 'target_column': 'is_deleted', 'target_type': 'BOOLEAN', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.is_deleted = sfeb.is_deleted', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.footfall_event_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'footfall_event_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.footfall_event_id = sfeb.footfall_event_id', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.store_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.store_id = sfeb.store_id', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.entry_count']", 'source_type': 'INT', 'source_nullable': 'NOT NULL', 'target_column': 'entry_count', 'target_type': 'INT', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.entry_count = sfeb.entry_count', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.exit_count']", 'source_type': 'INT', 'source_nullable': 'NOT NULL', 'target_column': 'exit_count', 'target_type': 'INT', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.exit_count = sfeb.exit_count', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.sensor_id']", 'source_type': 'STRING', 'source_nullable': 'NOT NULL', 'target_column': 'sensor_id', 'target_type': 'STRING', 'target_nullable': 'NOT NULL', 'transformation': 'sfeb.sensor_id = sfeb.sensor_id', 'target_table': 'sfeb'}
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    mapping_details = table.get('mapping_details', '')
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    source_table = None
    source_alias = None

    if isinstance(mapping_details, str):
        md_upper = mapping_details.upper()
        if 'SOURCE:' in md_upper:
            after_source = mapping_details.split('SOURCE:', 1)[1].strip()
            source_segment = after_source.split(';', 1)[0].strip()
            source_table = source_segment.split('(', 1)[0].strip()

    source_alias = target_alias

    df_reader = spark.read.format(read_format)
    if read_format == 'csv':
        df_reader = df_reader.option('header', 'true').option('inferSchema', 'true')

    df = df_reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            target_column = col_meta.get('target_column')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    df_writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        df_writer = df_writer.option('header', 'true')

    df_writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
