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
            'target_table': 'orders_bronze',
            'target_alias': 'ob',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Create orders bronze table by ingesting raw sales transactions at transaction grain. Map columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time directly from sales_transactions_raw.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customers_bronze',
            'target_alias': 'cb',
            'mapping_details': 'nan',
            'description': 'No customer source data provided in the available raw schemas; customers_bronze cannot be created from the given sources.'
        }
    ],
    'columns': [
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ob.transaction_id = str.transaction_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ob.store_id = str.store_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ob.product_id = str.product_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_null',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_null',
            'transformation': 'ob.quantity = str.quantity',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_null',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_null',
            'transformation': 'ob.sale_amount = str.sale_amount',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_null',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_null',
            'transformation': 'ob.transaction_time = str.transaction_time',
            'target_table': 'ob'
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
    target_table = table['target_table']
    target_alias = table['target_alias']
    mapping_details = table.get('mapping_details')

    if mapping_details is None or str(mapping_details).strip().lower() == 'nan' or str(mapping_details).strip() == '':
        continue

    parts = str(mapping_details).split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

    if source_table is None or source_alias is None:
        continue

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta.get('target_table') != target_alias:
            continue
        transformation = col_meta.get('transformation', '')
        if '=' not in transformation:
            continue
        rhs = transformation.split('=', 1)[1].strip()
        target_column = col_meta.get('target_column')
        if target_column is None:
            continue
        transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()