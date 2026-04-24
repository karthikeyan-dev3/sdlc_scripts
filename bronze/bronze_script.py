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
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw sr',
            'description': 'Bronze table for stores entity mapped directly from stores_raw: store_id, store_name, city, state, store_type, open_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze table for products entity mapped directly from products_raw: product_id, product_name, category, brand, price, is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze table for sales_transactions entity mapped directly from sales_transactions_raw: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'categories_bronze',
            'target_alias': 'cb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze table for categories entity derived as a raw replication of product category values from products_raw (no joins/aggregations): category.'
        }
    ],
    'columns': [
        {
            'source_column': "['sr.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'sr.store_id = sb.store_id',
            'target_table': 'sr'
        },
        {
            'source_column': "['sr.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'null_accepted',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'null_accepted',
            'transformation': 'sr.store_name = sb.store_name',
            'target_table': 'sr'
        },
        {
            'source_column': "['sr.city']",
            'source_type': 'STRING',
            'source_nullable': 'null_accepted',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'null_accepted',
            'transformation': 'sr.city = sb.city',
            'target_table': 'sr'
        },
        {
            'source_column': "['sr.state']",
            'source_type': 'STRING',
            'source_nullable': 'null_accepted',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'null_accepted',
            'transformation': 'sr.state = sb.state',
            'target_table': 'sr'
        },
        {
            'source_column': "['sr.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'null_accepted',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'null_accepted',
            'transformation': 'sr.store_type = sb.store_type',
            'target_table': 'sr'
        },
        {
            'source_column': "['sr.open_date']",
            'source_type': 'DATE',
            'source_nullable': 'null_accepted',
            'target_column': 'open_date',
            'target_type': 'DATE',
            'target_nullable': 'null_accepted',
            'transformation': 'sr.open_date = sb.open_date',
            'target_table': 'sr'
        },
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'pr.product_id = pb.product_id',
            'target_table': 'pr'
        },
        {
            'source_column': "['pr.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'null_accepted',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'null_accepted',
            'transformation': 'pr.product_name = pb.product_name',
            'target_table': 'pr'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'null_accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'null_accepted',
            'transformation': 'pr.category = pb.category',
            'target_table': 'pr'
        },
        {
            'source_column': "['pr.brand']",
            'source_type': 'STRING',
            'source_nullable': 'null_accepted',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'null_accepted',
            'transformation': 'pr.brand = pb.brand',
            'target_table': 'pr'
        },
        {
            'source_column': "['pr.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'null_accepted',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'null_accepted',
            'transformation': 'pr.price = pb.price',
            'target_table': 'pr'
        },
        {
            'source_column': "['pr.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'null_accepted',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'null_accepted',
            'transformation': 'pr.is_active = pb.is_active',
            'target_table': 'pr'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'str.transaction_id = stb.transaction_id',
            'target_table': 'str'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'str.store_id = stb.store_id',
            'target_table': 'str'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'str.product_id = stb.product_id',
            'target_table': 'str'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'null_accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'null_accepted',
            'transformation': 'str.quantity = stb.quantity',
            'target_table': 'str'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'null_accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'null_accepted',
            'transformation': 'str.sale_amount = stb.sale_amount',
            'target_table': 'str'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'null_accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'null_accepted',
            'transformation': 'str.transaction_time = stb.transaction_time',
            'target_table': 'str'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'null_accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'null_accepted',
            'transformation': 'pr.category = cb.category',
            'target_table': 'pr'
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
 
for table_meta in metadata.get('tables', []):
    mapping_details = table_meta.get('mapping_details', '')
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None
 
    target_table = table_meta.get('target_table')
    target_alias = table_meta.get('target_alias')
 
    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')
 
    df = reader.load(f"{base_path}{source_table}.{read_format}")
    df = df.alias(source_alias)
 
    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == source_alias and f"= {target_alias}." in col_meta.get('transformation', ''):
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[0].strip() if '=' in transformation else transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")
 
    df = df.selectExpr(*transformations)
 
    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')
 
    writer.save(f"{target_path}{target_table}.{write_format}")
 
job.commit()