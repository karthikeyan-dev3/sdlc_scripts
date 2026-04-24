from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

# Metadata provided to the job (do not hardcode any other configs)
metadata = {
    'tables': [
        {
            'target_schema': 'bronze',
            'target_table': 'customer_orders',
            'target_alias': 'co',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Create bronze customer_orders from sales_transactions_raw with 1:1 row mapping. Map: order_id=transaction_id, store_id=store_id, order_total_amount=sale_amount, order_time=transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_order_items',
            'target_alias': 'coi',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Create bronze customer_order_items from sales_transactions_raw with 1:1 row mapping at transaction line level. Map: order_id=transaction_id, product_id=product_id, quantity=quantity, line_amount=sale_amount.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'etl_load_audit',
            'target_alias': 'ela',
            'mapping_details': 'sales_transactions_raw str',
            'description': "Create bronze etl_load_audit capturing ingestion metadata for sales_transactions_raw loads only. Map: source_name='sales_transactions_raw', load_timestamp=CURRENT_TIMESTAMP, record_count=COUNT(*) (defer aggregation to later layers if needed), status, batch_id/run_id from orchestrator."
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'co.order_id = str.transaction_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'co.store_id = str.store_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_null',
            'target_column': 'order_total_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_null',
            'transformation': 'co.order_total_amount = str.sale_amount',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_null',
            'target_column': 'order_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_null',
            'transformation': 'co.order_time = str.transaction_time',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'coi.order_id = str.transaction_id',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'coi.product_id = str.product_id',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_null',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_null',
            'transformation': 'coi.quantity = str.quantity',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_null',
            'target_column': 'line_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_null',
            'transformation': 'coi.line_amount = str.sale_amount',
            'target_table': 'coi'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'source_name',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': "ela.source_name = 'sales_transactions_raw'",
            'target_table': 'ela'
        },
        {
            'source_column': '[]',
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_null',
            'target_column': 'load_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_null',
            'transformation': 'ela.load_timestamp = CURRENT_TIMESTAMP',
            'target_table': 'ela'
        },
        {
            'source_column': '[]',
            'source_type': 'BIGINT',
            'source_nullable': 'not_null',
            'target_column': 'record_count',
            'target_type': 'BIGINT',
            'target_nullable': 'not_null',
            'transformation': 'ela.record_count = COUNT(*)',
            'target_table': 'ela'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'status',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ela.status = <orchestrator_status>',
            'target_table': 'ela'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'batch_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ela.batch_id = <orchestrator_batch_id>',
            'target_table': 'ela'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'run_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ela.run_id = <orchestrator_run_id>',
            'target_table': 'ela'
        }
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table_meta in metadata['tables']:
    target_table = table_meta['target_table']
    target_alias = table_meta['target_alias']

    mapping_details = table_meta['mapping_details']
    source_table = mapping_details.split()[0]
    source_alias = mapping_details.split()[1]

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            transformation = col_meta['transformation']
            target_column = col_meta['target_column']
            rhs = transformation.split('=', 1)[1].strip()
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
