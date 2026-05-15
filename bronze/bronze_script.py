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
            'mapping_details': 'sales_event se + event_metadata em (from POS sales payload) -> 1:1 raw ingest (no joins, no aggregations). Columns: em.event_id, em.event_type, em.source_system, em.event_timestamp, em.ingestion_timestamp, em.batch_id, em.is_deleted, se.transaction_id, se.order_id, se.store_id, se.terminal_id, se.cashier_id, se.product_id, se.product_name, se.category, se.sub_category, se.quantity, se.unit_price, se.discount, se.total_amount, se.payment_id, se.event_action',
            'description': 'Bronze raw POS sales events captured with metadata. Keeps duplicates and late arrivals (e.g., same transaction_id with different event_id/ingestion_timestamp). Preserves source values and casing as received (e.g., category Dairy vs dairy).'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'payment_gateway_payment_event_bronze',
            'target_alias': 'pgpeb',
            'mapping_details': 'payment_event pe + event_metadata em (from PAYMENT_GATEWAY payment payload) -> 1:1 raw ingest (no joins, no aggregations). Columns: em.event_id, em.event_type, em.source_system, em.event_timestamp, em.ingestion_timestamp, em.batch_id, em.is_deleted, pe.payment_id, pe.transaction_id, pe.payment_mode, pe.provider, pe.amount, pe.currency, pe.payment_status',
            'description': 'Bronze raw payment events from payment gateway with metadata. Supports split payments per transaction_id (multiple payment_id records). Preserves null provider for cash and currency as received.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'inventory_system_inventory_event_bronze',
            'target_alias': 'isieb',
            'mapping_details': 'inventory_event ie + event_metadata em (from INVENTORY_SYSTEM inventory payload) -> 1:1 raw ingest (no joins, no aggregations). Columns: em.event_id, em.event_type, em.source_system, em.event_timestamp, em.ingestion_timestamp, em.batch_id, em.is_deleted, ie.inventory_event_id, ie.product_id, ie.store_id, ie.warehouse_id, ie.change_type, ie.quantity_changed, ie.current_stock',
            'description': 'Bronze raw inventory adjustment events with metadata. Captures stock changes such as returns without any enrichment or reconciliation.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sensor_footfall_event_bronze',
            'target_alias': 'sfeb',
            'mapping_details': 'footfall_event fe + event_metadata em (from SENSOR footfall payload) -> 1:1 raw ingest (no joins, no aggregations). Columns: em.event_id, em.event_type, em.source_system, em.event_timestamp, em.ingestion_timestamp, em.batch_id, em.is_deleted, fe.footfall_event_id, fe.store_id, fe.entry_count, fe.exit_count, fe.sensor_id',
            'description': 'Bronze raw footfall sensor events with metadata. Retains zero counts and original sensor identifiers as received.'
        }
    ],
    'columns': [
        {'source_column': "['pseb.event_id']", 'source_type': 'UUID', 'source_nullable': 'not_accepted', 'target_column': 'event_id', 'target_type': 'UUID', 'target_nullable': 'not_accepted', 'transformation': 'pseb.event_id = pseb.event_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.event_type']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'event_type', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.event_type = pseb.event_type', 'target_table': 'pseb'},
        {'source_column': "['pseb.source_system']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.source_system = pseb.source_system', 'target_table': 'pseb'},
        {'source_column': "['pseb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'pseb.event_timestamp = pseb.event_timestamp', 'target_table': 'pseb'},
        {'source_column': "['pseb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'ingestion_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'pseb.ingestion_timestamp = pseb.ingestion_timestamp', 'target_table': 'pseb'},
        {'source_column': "['pseb.batch_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'batch_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.batch_id = pseb.batch_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.is_deleted']", 'source_type': 'BOOLEAN', 'source_nullable': 'not_accepted', 'target_column': 'is_deleted', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'pseb.is_deleted = pseb.is_deleted', 'target_table': 'pseb'},
        {'source_column': "['pseb.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.transaction_id = pseb.transaction_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.order_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.order_id = pseb.order_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.store_id = pseb.store_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.terminal_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'terminal_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.terminal_id = pseb.terminal_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.cashier_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'cashier_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.cashier_id = pseb.cashier_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.product_id = pseb.product_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.product_name']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.product_name = pseb.product_name', 'target_table': 'pseb'},
        {'source_column': "['pseb.category']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'category', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.category = pseb.category', 'target_table': 'pseb'},
        {'source_column': "['pseb.sub_category']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'sub_category', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.sub_category = pseb.sub_category', 'target_table': 'pseb'},
        {'source_column': "['pseb.quantity']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'pseb.quantity = pseb.quantity', 'target_table': 'pseb'},
        {'source_column': "['pseb.unit_price']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'not_accepted', 'target_column': 'unit_price', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'not_accepted', 'transformation': 'pseb.unit_price = pseb.unit_price', 'target_table': 'pseb'},
        {'source_column': "['pseb.discount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'accepted', 'target_column': 'discount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'accepted', 'transformation': 'pseb.discount = pseb.discount', 'target_table': 'pseb'},
        {'source_column': "['pseb.total_amount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'not_accepted', 'target_column': 'total_amount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'not_accepted', 'transformation': 'pseb.total_amount = pseb.total_amount', 'target_table': 'pseb'},
        {'source_column': "['pseb.payment_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'payment_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.payment_id = pseb.payment_id', 'target_table': 'pseb'},
        {'source_column': "['pseb.event_action']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'event_action', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.event_action = pseb.event_action', 'target_table': 'pseb'},
        {'source_column': "['pgpeb.event_id']", 'source_type': 'UUID', 'source_nullable': 'not_accepted', 'target_column': 'event_id', 'target_type': 'UUID', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.event_id = pgpeb.event_id', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.event_type']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'event_type', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.event_type = pgpeb.event_type', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.source_system']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.source_system = pgpeb.source_system', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.event_timestamp = pgpeb.event_timestamp', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'ingestion_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.ingestion_timestamp = pgpeb.ingestion_timestamp', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.batch_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'batch_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.batch_id = pgpeb.batch_id', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.is_deleted']", 'source_type': 'BOOLEAN', 'source_nullable': 'not_accepted', 'target_column': 'is_deleted', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.is_deleted = pgpeb.is_deleted', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.payment_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'payment_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.payment_id = pgpeb.payment_id', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.transaction_id = pgpeb.transaction_id', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.payment_mode']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'payment_mode', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pgpeb.payment_mode = pgpeb.payment_mode', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.provider']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'provider', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pgpeb.provider = pgpeb.provider', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.amount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'not_accepted', 'target_column': 'amount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'not_accepted', 'transformation': 'pgpeb.amount = pgpeb.amount', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.currency']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'currency', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pgpeb.currency = pgpeb.currency', 'target_table': 'pgpeb'},
        {'source_column': "['pgpeb.payment_status']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'payment_status', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pgpeb.payment_status = pgpeb.payment_status', 'target_table': 'pgpeb'},
        {'source_column': "['isieb.event_id']", 'source_type': 'UUID', 'source_nullable': 'not_accepted', 'target_column': 'event_id', 'target_type': 'UUID', 'target_nullable': 'not_accepted', 'transformation': 'isieb.event_id = isieb.event_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.event_type']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'event_type', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'isieb.event_type = isieb.event_type', 'target_table': 'isieb'},
        {'source_column': "['isieb.source_system']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'isieb.source_system = isieb.source_system', 'target_table': 'isieb'},
        {'source_column': "['isieb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'isieb.event_timestamp = isieb.event_timestamp', 'target_table': 'isieb'},
        {'source_column': "['isieb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'ingestion_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'isieb.ingestion_timestamp = isieb.ingestion_timestamp', 'target_table': 'isieb'},
        {'source_column': "['isieb.batch_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'batch_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'isieb.batch_id = isieb.batch_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.is_deleted']", 'source_type': 'BOOLEAN', 'source_nullable': 'not_accepted', 'target_column': 'is_deleted', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'isieb.is_deleted = isieb.is_deleted', 'target_table': 'isieb'},
        {'source_column': "['isieb.inventory_event_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'inventory_event_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'isieb.inventory_event_id = isieb.inventory_event_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'isieb.product_id = isieb.product_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'isieb.store_id = isieb.store_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.warehouse_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'warehouse_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'isieb.warehouse_id = isieb.warehouse_id', 'target_table': 'isieb'},
        {'source_column': "['isieb.change_type']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'change_type', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'isieb.change_type = isieb.change_type', 'target_table': 'isieb'},
        {'source_column': "['isieb.quantity_changed']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'quantity_changed', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'isieb.quantity_changed = isieb.quantity_changed', 'target_table': 'isieb'},
        {'source_column': "['isieb.current_stock']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'current_stock', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'isieb.current_stock = isieb.current_stock', 'target_table': 'isieb'},
        {'source_column': "['sfeb.event_id']", 'source_type': 'UUID', 'source_nullable': 'not_accepted', 'target_column': 'event_id', 'target_type': 'UUID', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.event_id = sfeb.event_id', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.event_type']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'event_type', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.event_type = sfeb.event_type', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.source_system']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'source_system', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.source_system = sfeb.source_system', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'event_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.event_timestamp = sfeb.event_timestamp', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.ingestion_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'ingestion_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.ingestion_timestamp = sfeb.ingestion_timestamp', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.batch_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'batch_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.batch_id = sfeb.batch_id', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.is_deleted']", 'source_type': 'BOOLEAN', 'source_nullable': 'not_accepted', 'target_column': 'is_deleted', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.is_deleted = sfeb.is_deleted', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.footfall_event_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'footfall_event_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.footfall_event_id = sfeb.footfall_event_id', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.store_id = sfeb.store_id', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.entry_count']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'entry_count', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.entry_count = sfeb.entry_count', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.exit_count']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'exit_count', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'sfeb.exit_count = sfeb.exit_count', 'target_table': 'sfeb'},
        {'source_column': "['sfeb.sensor_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'sensor_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'sfeb.sensor_id = sfeb.sensor_id', 'target_table': 'sfeb'}
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table in metadata['tables']:
    target_table = table['target_table']
    source_alias = table['target_alias']
    mapping_details = table.get('mapping_details', '')

    source_table = None
    if isinstance(mapping_details, str):
        source_table = mapping_details.split('->')[0].strip() if '->' in mapping_details else mapping_details.strip()

    read_builder = spark.read.format(read_format)
    if read_format == 'csv':
        read_builder = read_builder.option("header", "true").option("inferSchema", "true")

    df = read_builder.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == source_alias:
            transformation = col_meta['transformation']
            target_column = col_meta['target_column']
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            rhs = rhs.replace("sales_date", "transaction_date")
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    write_builder = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        write_builder = write_builder.option("header", "true")

    write_builder.save(target_path + target_table + "." + write_format)

job.commit()
