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
            'target_alias': 'pmb',
            'mapping_details': 'products_raw p',
            'description': 'Bronze table for Product Master entity. Direct mapping from products_raw with columns: product_id, product_name, category, brand, price, is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'store_master_bronze',
            'target_alias': 'smb',
            'mapping_details': 'stores_raw s',
            'description': 'Bronze table for Store Master entity. Direct mapping from stores_raw with columns: store_id, store_name, city, state, store_type, open_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sales_performance_bronze',
            'target_alias': 'spb',
            'mapping_details': 'sales_transactions_raw t',
            'description': 'Bronze table for Sales Performance entity. Direct mapping from sales_transactions_raw with columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        }
    ],
    'columns': [
        {
            'source_column': "['p.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'null_accepted',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'null_accepted',
            'transformation': 'p.product_id = pmb.product_id',
            'target_table': 'p'
        },
        {
            'source_column': "['p.product_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'null_accepted',
            'target_column': 'product_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'null_accepted',
            'transformation': 'p.product_name = pmb.product_name',
            'target_table': 'p'
        },
        {
            'source_column': "['p.category']",
            'source_type': 'varchar(100)',
            'source_nullable': 'null_accepted',
            'target_column': 'category',
            'target_type': 'varchar(100)',
            'target_nullable': 'null_accepted',
            'transformation': 'p.category = pmb.category',
            'target_table': 'p'
        },
        {
            'source_column': "['p.brand']",
            'source_type': 'varchar(100)',
            'source_nullable': 'null_accepted',
            'target_column': 'brand',
            'target_type': 'varchar(100)',
            'target_nullable': 'null_accepted',
            'transformation': 'p.brand = pmb.brand',
            'target_table': 'p'
        },
        {
            'source_column': "['p.price']",
            'source_type': 'float',
            'source_nullable': 'null_accepted',
            'target_column': 'price',
            'target_type': 'float',
            'target_nullable': 'null_accepted',
            'transformation': 'p.price = pmb.price',
            'target_table': 'p'
        },
        {
            'source_column': "['p.is_active']",
            'source_type': 'boolean',
            'source_nullable': 'null_accepted',
            'target_column': 'is_active',
            'target_type': 'boolean',
            'target_nullable': 'null_accepted',
            'transformation': 'p.is_active = pmb.is_active',
            'target_table': 'p'
        },
        {
            'source_column': "['s.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'null_accepted',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'null_accepted',
            'transformation': 's.store_id = smb.store_id',
            'target_table': 's'
        },
        {
            'source_column': "['s.store_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'null_accepted',
            'target_column': 'store_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'null_accepted',
            'transformation': 's.store_name = smb.store_name',
            'target_table': 's'
        },
        {
            'source_column': "['s.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'null_accepted',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'null_accepted',
            'transformation': 's.city = smb.city',
            'target_table': 's'
        },
        {
            'source_column': "['s.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'null_accepted',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'null_accepted',
            'transformation': 's.state = smb.state',
            'target_table': 's'
        },
        {
            'source_column': "['s.store_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'null_accepted',
            'target_column': 'store_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'null_accepted',
            'transformation': 's.store_type = smb.store_type',
            'target_table': 's'
        },
        {
            'source_column': "['s.open_date']",
            'source_type': 'date',
            'source_nullable': 'null_accepted',
            'target_column': 'open_date',
            'target_type': 'date',
            'target_nullable': 'null_accepted',
            'transformation': 's.open_date = smb.open_date',
            'target_table': 's'
        },
        {
            'source_column': "['t.transaction_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'null_accepted',
            'target_column': 'transaction_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'null_accepted',
            'transformation': 't.transaction_id = spb.transaction_id',
            'target_table': 't'
        },
        {
            'source_column': "['t.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'null_accepted',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'null_accepted',
            'transformation': 't.store_id = spb.store_id',
            'target_table': 't'
        },
        {
            'source_column': "['t.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'null_accepted',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'null_accepted',
            'transformation': 't.product_id = spb.product_id',
            'target_table': 't'
        },
        {
            'source_column': "['t.quantity']",
            'source_type': 'int',
            'source_nullable': 'null_accepted',
            'target_column': 'quantity',
            'target_type': 'int',
            'target_nullable': 'null_accepted',
            'transformation': 't.quantity = spb.quantity',
            'target_table': 't'
        },
        {
            'source_column': "['t.sale_amount']",
            'source_type': 'double',
            'source_nullable': 'null_accepted',
            'target_column': 'sale_amount',
            'target_type': 'double',
            'target_nullable': 'null_accepted',
            'transformation': 't.sale_amount = spb.sale_amount',
            'target_table': 't'
        },
        {
            'source_column': "['t.transaction_time']",
            'source_type': 'timestamp',
            'source_nullable': 'null_accepted',
            'target_column': 'transaction_time',
            'target_type': 'timestamp',
            'target_nullable': 'null_accepted',
            'transformation': 't.transaction_time = spb.transaction_time',
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

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table in metadata['tables']:
    target_table = table['target_table']
    target_alias = table['target_alias']

    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1] if len(mapping_details) > 1 else mapping_details[0]

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta.get('target_table') == source_alias:
            rhs = col_meta['transformation'].split('=', 1)[1].strip()
            target_column = col_meta['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()