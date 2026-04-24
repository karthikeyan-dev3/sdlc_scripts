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
            'target_table': 'customer_orders_daily',
            'target_alias': 'cod',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze table to capture raw daily customer order transactions. Direct ingestion from sales_transactions_raw without joins or aggregations. Includes transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_orders_load_audit',
            'target_alias': 'cola',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze load-audit table to record ingestion events for customer_orders_daily sourced from sales_transactions_raw (e.g., load timestamp, source identifier, record counts, status). No joins or aggregations.'
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
            'transformation': 'cod.transaction_id = str.transaction_id',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'cod.store_id = str.store_id',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'cod.product_id = str.product_id',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'cod.quantity = str.quantity',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'cod.sale_amount = str.sale_amount',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'cod.transaction_time = str.transaction_time',
            'target_table': 'cod'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'load_ts',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'cola.load_ts = CURRENT_TIMESTAMP()',
            'target_table': 'cola'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'source_record_count',
            'target_type': 'BIGINT',
            'target_nullable': 'not_accepted',
            'transformation': 'cola.source_record_count = COUNT(str.transaction_id)',
            'target_table': 'cola'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'loaded_record_count',
            'target_type': 'BIGINT',
            'target_nullable': 'not_accepted',
            'transformation': 'cola.loaded_record_count = COUNT(str.transaction_id)',
            'target_table': 'cola'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'load_status',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': "cola.load_status = 'SUCCESS'",
            'target_table': 'cola'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'source_table',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': "cola.source_table = 'sales_transactions_raw'",
            'target_table': 'cola'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'min_transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'cola.min_transaction_time = MIN(str.transaction_time)',
            'target_table': 'cola'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'max_transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'cola.max_transaction_time = MAX(str.transaction_time)',
            'target_table': 'cola'
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
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + '.' + read_format)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            if '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
            else:
                rhs = transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()
