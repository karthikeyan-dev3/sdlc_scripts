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
            'description': 'Bronze customer_orders table sourced directly from sales_transactions_raw; includes transaction_id as order identifier, store_id, sale_amount, and transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_order_items',
            'target_alias': 'coi',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze customer_order_items table sourced directly from sales_transactions_raw; includes transaction_id as order identifier, product_id, and quantity.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_data_quality_daily',
            'target_alias': 'odqd',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze order_data_quality_daily table sourced directly from sales_transactions_raw; retains raw transactional records needed for downstream data quality checks by date derived from transaction_time.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not',
            'transformation': 'co.transaction_id = str.transaction_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not',
            'transformation': 'co.store_id = str.store_id',
            'target_table': 'co'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not',
            'transformation': 'co.sale_amount = str.sale_amount',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not',
            'transformation': 'co.transaction_time = str.transaction_time',
            'target_table': 'co'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not',
            'transformation': 'coi.transaction_id = str.transaction_id',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not',
            'transformation': 'coi.product_id = str.product_id',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not',
            'transformation': 'coi.quantity = str.quantity',
            'target_table': 'coi'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not',
            'target_column': 'transaction_date',
            'target_type': 'DATE',
            'target_nullable': 'not',
            'transformation': 'odqd.transaction_date = CAST(str.transaction_time AS DATE)',
            'target_table': 'odqd'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not',
            'transformation': 'odqd.transaction_id = str.transaction_id',
            'target_table': 'odqd'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not',
            'transformation': 'odqd.store_id = str.store_id',
            'target_table': 'odqd'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not',
            'transformation': 'odqd.product_id = str.product_id',
            'target_table': 'odqd'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not',
            'transformation': 'odqd.quantity = str.quantity',
            'target_table': 'odqd'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not',
            'transformation': 'odqd.sale_amount = str.sale_amount',
            'target_table': 'odqd'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not',
            'transformation': 'odqd.transaction_time = str.transaction_time',
            'target_table': 'odqd'
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
    mapping_details = table.get('mapping_details', '')
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None

    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

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
            target_col = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()
