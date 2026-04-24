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
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'SELECT transaction_id, store_id, product_id, quantity, sale_amount, transaction_time FROM sales_transactions_raw',
            'description': 'Bronze table capturing raw sales transaction events at transaction grain with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'SELECT product_id, product_name, category, brand, price, is_active FROM products_raw',
            'description': 'Bronze table capturing raw product master data with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'SELECT store_id, store_name, city, state, store_type, open_date FROM stores_raw',
            'description': 'Bronze table capturing raw store master data with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'ingestion_file_audit_bronze',
            'target_alias': 'ifab',
            'mapping_details': 'SELECT * FROM ingestion_file_audit',
            'description': 'Bronze audit table capturing file-level ingestion metadata exactly as recorded by the ingestion framework.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'ingestion_batch_summary_bronze',
            'target_alias': 'ibsb',
            'mapping_details': 'SELECT * FROM ingestion_batch_summary',
            'description': 'Bronze operational table capturing batch-level ingestion run summaries exactly as recorded by the ingestion framework.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'data_quality_results_bronze',
            'target_alias': 'dqrb',
            'mapping_details': 'SELECT * FROM data_quality_results',
            'description': 'Bronze table capturing raw data quality check outputs and rule evaluation results with no transformations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sensitive_data_access_audit_bronze',
            'target_alias': 'sdaab',
            'mapping_details': 'SELECT * FROM sensitive_data_access_audit',
            'description': 'Bronze audit table capturing raw access events to sensitive datasets as logged by the security/auditing system.'
        }
    ],
    'columns': [
        {
            'source_column': "['stb.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'stb.transaction_id = sales_transactions_raw.transaction_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'stb.store_id = sales_transactions_raw.store_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'stb.product_id = sales_transactions_raw.product_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not specified',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not specified',
            'transformation': 'stb.quantity = sales_transactions_raw.quantity',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not specified',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not specified',
            'transformation': 'stb.sale_amount = sales_transactions_raw.sale_amount',
            'target_table': 'stb'
        },
        {
            'source_column': "['stb.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not specified',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not specified',
            'transformation': 'stb.transaction_time = sales_transactions_raw.transaction_time',
            'target_table': 'stb'
        },
        {
            'source_column': "['pb.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'pb.product_id = products_raw.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'pb.product_name = products_raw.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.category']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'pb.category = products_raw.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.brand']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'pb.brand = products_raw.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not specified',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'not specified',
            'transformation': 'pb.price = products_raw.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'not specified',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'not specified',
            'transformation': 'pb.is_active = products_raw.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['sb.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'sb.store_id = stores_raw.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'sb.store_name = stores_raw.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.city']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'sb.city = stores_raw.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.state']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'sb.state = stores_raw.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'not specified',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'not specified',
            'transformation': 'sb.store_type = stores_raw.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.open_date']",
            'source_type': 'DATE',
            'source_nullable': 'not specified',
            'target_column': 'open_date',
            'target_type': 'DATE',
            'target_nullable': 'not specified',
            'transformation': 'sb.open_date = stores_raw.open_date',
            'target_table': 'sb'
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
    target_table = table.get('target_table')

    mapping_upper = mapping_details.upper()
    if ' FROM ' not in mapping_upper:
        continue

    # Extract source table from mapping_details (after FROM)
    from_idx = mapping_upper.rfind(' FROM ')
    source_table = mapping_details[from_idx + len(' FROM '):].strip()

    # Extract source alias (target_alias)
    source_alias = table.get('target_alias')

    # Read
    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + f"{source_table}.{read_format}")

    # Apply alias
    df = df.alias(source_alias)

    # Column transformations for this alias
    transformations = []
    for col in metadata.get('columns', []):
        if col.get('target_table') == source_alias:
            transformation = col.get('transformation', '')
            if '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
            else:
                rhs = transformation.strip()
            target_col = col.get('target_column')
            transformations.append(f"{rhs} as {target_col}")

    if transformations:
        df = df.selectExpr(*transformations)

    # Write
    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
