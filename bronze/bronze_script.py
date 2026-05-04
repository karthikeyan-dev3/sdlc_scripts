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
        {'target_schema': 'bronze', 'target_table': 'pos_sales_event_bronze', 'target_alias': 'pseb', 'mapping_details': 'source_schemas.event_metadata em, source_schemas.sales_event se (1:1 within each source record; no joins)'},
        {'target_schema': 'bronze', 'target_table': 'payment_gateway_payment_event_bronze', 'target_alias': 'pgpeb', 'mapping_details': 'source_schemas.event_metadata em, source_schemas.payment_event pe (1:1 within each source record; no joins)'},
        {'target_schema': 'bronze', 'target_table': 'inventory_system_inventory_event_bronze', 'target_alias': 'isieb', 'mapping_details': 'source_schemas.event_metadata em, source_schemas.inventory_event ie (1:1 within each source record; no joins)'},
        {'target_schema': 'bronze', 'target_table': 'sensor_footfall_event_bronze', 'target_alias': 'sfeb', 'mapping_details': 'source_schemas.event_metadata em, source_schemas.footfall_event fe (1:1 within each source record; no joins)'}
    ],
    'columns': [
        {'target_table': 'pseb', 'transformation': 'pseb.event_id = em.event_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.event_type = em.event_type'},
        {'target_table': 'pseb', 'transformation': 'pseb.source_system = em.source_system'},
        {'target_table': 'pseb', 'transformation': 'pseb.event_timestamp = em.event_timestamp'},
        {'target_table': 'pseb', 'transformation': 'pseb.ingestion_timestamp = em.ingestion_timestamp'},
        {'target_table': 'pseb', 'transformation': 'pseb.batch_id = em.batch_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.is_deleted = em.is_deleted'},
        {'target_table': 'pseb', 'transformation': 'pseb.transaction_id = se.transaction_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.order_id = se.order_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.store_id = se.store_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.terminal_id = se.terminal_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.cashier_id = se.cashier_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.product_id = se.product_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.product_name = se.product_name'},
        {'target_table': 'pseb', 'transformation': 'pseb.category = se.category'},
        {'target_table': 'pseb', 'transformation': 'pseb.sub_category = se.sub_category'},
        {'target_table': 'pseb', 'transformation': 'pseb.quantity = se.quantity'},
        {'target_table': 'pseb', 'transformation': 'pseb.unit_price = se.unit_price'},
        {'target_table': 'pseb', 'transformation': 'pseb.discount = se.discount'},
        {'target_table': 'pseb', 'transformation': 'pseb.total_amount = se.total_amount'},
        {'target_table': 'pseb', 'transformation': 'pseb.payment_id = se.payment_id'},
        {'target_table': 'pseb', 'transformation': 'pseb.event_action = se.event_action'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.event_id = em.event_id'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.event_type = em.event_type'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.source_system = em.source_system'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.event_timestamp = em.event_timestamp'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.ingestion_timestamp = em.ingestion_timestamp'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.batch_id = em.batch_id'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.is_deleted = em.is_deleted'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.payment_id = pe.payment_id'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.transaction_id = pe.transaction_id'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.payment_mode = pe.payment_mode'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.provider = pe.provider'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.amount = pe.amount'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.currency = pe.currency'},
        {'target_table': 'pgpeb', 'transformation': 'pgpeb.payment_status = pe.payment_status'},
        {'target_table': 'isieb', 'transformation': 'isieb.event_id = em.event_id'},
        {'target_table': 'isieb', 'transformation': 'isieb.event_type = em.event_type'},
        {'target_table': 'isieb', 'transformation': 'isieb.source_system = em.source_system'},
        {'target_table': 'isieb', 'transformation': 'isieb.event_timestamp = em.event_timestamp'},
        {'target_table': 'isieb', 'transformation': 'isieb.ingestion_timestamp = em.ingestion_timestamp'},
        {'target_table': 'isieb', 'transformation': 'isieb.batch_id = em.batch_id'},
        {'target_table': 'isieb', 'transformation': 'isieb.is_deleted = em.is_deleted'},
        {'target_table': 'isieb', 'transformation': 'isieb.inventory_event_id = ie.inventory_event_id'},
        {'target_table': 'isieb', 'transformation': 'isieb.product_id = ie.product_id'},
        {'target_table': 'isieb', 'transformation': 'isieb.store_id = ie.store_id'},
        {'target_table': 'isieb', 'transformation': 'isieb.warehouse_id = ie.warehouse_id'},
        {'target_table': 'isieb', 'transformation': 'isieb.change_type = ie.change_type'},
        {'target_table': 'isieb', 'transformation': 'isieb.quantity_changed = ie.quantity_changed'},
        {'target_table': 'isieb', 'transformation': 'isieb.current_stock = ie.current_stock'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.event_id = em.event_id'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.event_type = em.event_type'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.source_system = em.source_system'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.event_timestamp = em.event_timestamp'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.ingestion_timestamp = em.ingestion_timestamp'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.batch_id = em.batch_id'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.is_deleted = em.is_deleted'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.footfall_event_id = fe.footfall_event_id'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.store_id = fe.store_id'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.entry_count = fe.entry_count'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.exit_count = fe.exit_count'},
        {'target_table': 'sfeb', 'transformation': 'sfeb.sensor_id = fe.sensor_id'}
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

# Go through each table in the metadata
tables = metadata['tables']
for table in tables:
    target_table = table['target_table']
    target_alias = table['target_alias']
    # Extract source tables from mapping details
    source_tables = table['mapping_details'].split(',', 1)
    
    source_table_main = source_tables[0].strip().split(' ')[-1]

    # Load the main dataframe
df = spark.read.format(metadata['runtime_config']['read_format']) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(metadata['runtime_config']['base_path'] + source_table_main + '.' + metadata['runtime_config']['read_format'])

    # Apply alias
df = df.alias(source_table_main)

    # Filter columns for the current target table and extract transformations
    transformations = [
        col['transformation'].split('=', 1)[1].strip() + ' as ' + col['target_column']
        for col in metadata['columns'] if col['target_table'] == target_alias
    ]

    # Apply transformations and select the required columns
df = df.selectExpr(*transformations)

    # Save the transformed dataframe to its target location
df.write.mode(metadata['runtime_config']['write_mode']) \
        .format(metadata['runtime_config']['write_format']) \
        .option("header", "true") \
        .save(metadata['runtime_config']['target_path'] + target_table + '/' + metadata['runtime_config']['write_format'])

job.commit()