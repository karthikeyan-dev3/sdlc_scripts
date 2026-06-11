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
            'target_table': 'product_master_bronze',
            'target_alias': 'p',
            'mapping_details': 'products_raw p',
            'description': 'Bronze ingestion of product master data. Direct copy of products_raw columns: product_id, product_name, category, brand, price, is_active. Use p.product_id (not pmb.product_id).'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'store_master_bronze',
            'target_alias': 's',
            'mapping_details': 'stores_raw s',
            'description': 'Bronze ingestion of store master data. Direct copy of stores_raw columns: store_id, store_name, city, state, store_type, open_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sales_transactions_bronze',
            'target_alias': 't',
            'mapping_details': 'sales_transactions_raw t',
            'description': 'Bronze ingestion of sales transactions data at transaction grain. Direct copy of sales_transactions_raw columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        }
    ],
    'columns': [
        {
            'source_column': "['p.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 'p.product_id = p.product_id',
            'target_table': 'p'
        },
        {
            'source_column': "['p.product_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'product_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'p.product_name = p.product_name',
            'target_table': 'p'
        },
        {
            'source_column': "['p.category']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'category',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'p.category = p.category',
            'target_table': 'p'
        },
        {
            'source_column': "['p.brand']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'brand',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'p.brand = p.brand',
            'target_table': 'p'
        },
        {
            'source_column': "['p.price']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'price',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'p.price = p.price',
            'target_table': 'p'
        },
        {
            'source_column': "['p.is_active']",
            'source_type': 'boolean',
            'source_nullable': 'not specified',
            'target_column': 'is_active',
            'target_type': 'boolean',
            'target_nullable': 'not specified',
            'transformation': 'p.is_active = p.is_active',
            'target_table': 'p'
        },
        {
            'source_column': "['s.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 's.store_id = s.store_id',
            'target_table': 's'
        },
        {
            'source_column': "['s.store_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'store_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 's.store_name = s.store_name',
            'target_table': 's'
        },
        {
            'source_column': "['s.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 's.city = s.city',
            'target_table': 's'
        },
        {
            'source_column': "['s.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 's.state = s.state',
            'target_table': 's'
        },
        {
            'source_column': "['s.store_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not specified',
            'target_column': 'store_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'not specified',
            'transformation': 's.store_type = s.store_type',
            'target_table': 's'
        },
        {
            'source_column': "['s.open_date']",
            'source_type': 'date',
            'source_nullable': 'not specified',
            'target_column': 'open_date',
            'target_type': 'date',
            'target_nullable': 'not specified',
            'transformation': 's.open_date = s.open_date',
            'target_table': 's'
        },
        {
            'source_column': "['t.transaction_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'transaction_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 't.transaction_id = t.transaction_id',
            'target_table': 't'
        },
        {
            'source_column': "['t.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 't.store_id = t.store_id',
            'target_table': 't'
        },
        {
            'source_column': "['t.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 't.product_id = t.product_id',
            'target_table': 't'
        },
        {
            'source_column': "['t.quantity']",
            'source_type': 'int',
            'source_nullable': 'not specified',
            'target_column': 'quantity',
            'target_type': 'int',
            'target_nullable': 'not specified',
            'transformation': 't.quantity = t.quantity',
            'target_table': 't'
        },
        {
            'source_column': "['t.sale_amount']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'sale_amount',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 't.sale_amount = t.sale_amount',
            'target_table': 't'
        },
        {
            'source_column': "['t.transaction_time']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'transaction_time',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 't.transaction_time = t.transaction_time',
            'target_table': 't'
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
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()