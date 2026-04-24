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
            'target_table': 'customer_orders',
            'target_alias': 'co',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Raw capture of sales transactions at the order/transaction grain. Map: co.transaction_id <- str.transaction_id; co.store_id <- str.store_id; co.transaction_time <- str.transaction_time; co.sale_amount <- str.sale_amount.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_order_line_items',
            'target_alias': 'coli',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Raw capture of line items from sales transactions (one row per transaction_id + product_id from source). Map: coli.transaction_id <- str.transaction_id; coli.product_id <- str.product_id; coli.quantity <- str.quantity.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'etl_audit_lineage',
            'target_alias': 'eal',
            'mapping_details': 'sales_transactions_raw str; products_raw pr; stores_raw sr',
            'description': 'ETL audit/lineage capture for bronze ingestion runs of each raw source. Record source_system/table name, load timestamps, run_id/batch_id, row counts, and status for str, pr, and sr ingestions.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'co.transaction_id = str.transaction_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'co.store_id = str.store_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'co.transaction_time = str.transaction_time',
            'target_table': 'co'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'co.sale_amount = str.sale_amount',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'coli.transaction_id = str.transaction_id',
            'target_table': 'coli'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'coli.product_id = str.product_id',
            'target_table': 'coli'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'coli.quantity = str.quantity',
            'target_table': 'coli'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'source_table',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': "eal.source_table = 'sales_transactions_raw'|'products_raw'|'stores_raw'",
            'target_table': 'eal'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'source_schema',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': "eal.source_schema = 'raw'",
            'target_table': 'eal'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'run_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'eal.run_id = pipeline_run_id',
            'target_table': 'eal'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'batch_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'eal.batch_id = ingestion_batch_id',
            'target_table': 'eal'
        },
        {
            'source_column': '[]',
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'load_start_ts',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'eal.load_start_ts = current_timestamp_at_ingestion_start',
            'target_table': 'eal'
        },
        {
            'source_column': '[]',
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'load_end_ts',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'eal.load_end_ts = current_timestamp_at_ingestion_end',
            'target_table': 'eal'
        },
        {
            'source_column': '[]',
            'source_type': 'BIGINT',
            'source_nullable': 'not_accepted',
            'target_column': 'row_count',
            'target_type': 'BIGINT',
            'target_nullable': 'not_accepted',
            'transformation': 'eal.row_count = count(*) from str|pr|sr',
            'target_table': 'eal'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'status',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': "eal.status = 'SUCCESS'|'FAILED'",
            'target_table': 'eal'
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

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')
    mapping_details = table.get('mapping_details', '')

    first_mapping = mapping_details.split(';')[0].strip() if mapping_details else ''
    parts = first_mapping.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(f"{base_path}{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col in metadata.get('columns', []):
        if col.get('target_table') == target_alias:
            transformation = col.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()
