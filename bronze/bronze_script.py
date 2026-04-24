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
            'target_table': 'customer_orders_bronze',
            'target_alias': 'cob',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze capture of customer_orders from sales_transactions_raw. Map: cob.order_id = str.transaction_id; cob.store_id = str.store_id; cob.product_id = str.product_id; cob.quantity = str.quantity; cob.sale_amount = str.sale_amount; cob.order_timestamp = str.transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_status_bronze',
            'target_alias': 'osb',
            'mapping_details': 'sales_transactions_raw str',
            'description': "Bronze capture of order_status from sales_transactions_raw at transaction level. Map: osb.order_id = str.transaction_id; osb.status = 'COMPLETED' (derived as transaction exists); osb.status_timestamp = str.transaction_time."
        },
        {
            'target_schema': 'bronze',
            'target_table': 'currencies_bronze',
            'target_alias': 'cub',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze capture of currencies cannot be sourced from provided schemas (no currency code/amount currency fields). No direct mapping available from sales_transactions_raw/products_raw/stores_raw.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customers_bronze',
            'target_alias': 'cusb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze capture of customers cannot be sourced from provided schemas (no customer identifier/attributes). No direct mapping available from sales_transactions_raw/products_raw/stores_raw.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'source_systems_bronze',
            'target_alias': 'ssb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze capture of source_systems cannot be sourced from provided schemas (no source system identifiers/metadata). No direct mapping available from sales_transactions_raw/products_raw/stores_raw.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.order_id = str.transaction_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.store_id = str.store_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.product_id = str.product_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.quantity = str.quantity',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.sale_amount = str.sale_amount',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'order_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.order_timestamp = str.transaction_time',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'osb.order_id = str.transaction_id',
            'target_table': 'osb'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'status',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': "osb.status = 'COMPLETED'",
            'target_table': 'osb'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'status_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'osb.status_timestamp = str.transaction_time',
            'target_table': 'osb'
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

read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details']
    source_table, source_alias = mapping_details.split()
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(f"{base_path}{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col in metadata['columns']:
        if col['target_table'] == target_alias:
            transformation = col['transformation']
            rhs = transformation.split('=', 1)[1].strip()
            target_col = col['target_column']
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()