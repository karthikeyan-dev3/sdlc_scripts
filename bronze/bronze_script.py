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
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze table for sales transactions. Direct mapping from sales_transactions_raw with columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw sr',
            'description': 'Bronze table for stores. Direct mapping from stores_raw with columns: store_id, store_name, city, state, store_type, open_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze table for products. Direct mapping from products_raw with columns: product_id, product_name, category, brand, price, is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'product_categories_bronze',
            'target_alias': 'pcb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze table for product categories derived directly from products_raw category field (no joins/aggregations). Includes category as provided in source.'
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
            'transformation': 'stb.transaction_id = str.transaction_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'stb.store_id = str.store_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'stb.product_id = str.product_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'stb.quantity = str.quantity',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'stb.sale_amount = str.sale_amount',
            'target_table': 'stb'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'stb.transaction_time = str.transaction_time',
            'target_table': 'stb'
        },
        {
            'source_column': "['sr.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'sb.store_id = sr.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_name = sr.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.city']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.city = sr.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.state']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.state = sr.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_type = sr.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['sr.open_date']",
            'source_type': 'DATE',
            'source_nullable': 'accepted',
            'target_column': 'open_date',
            'target_type': 'DATE',
            'target_nullable': 'accepted',
            'transformation': 'sb.open_date = sr.open_date',
            'target_table': 'sb'
        },
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.product_id = pr.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.product_name = pr.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.category = pr.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.brand']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pb.brand = pr.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'pb.price = pr.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'accepted',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'accepted',
            'transformation': 'pb.is_active = pr.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'pcb.category = pr.category',
            'target_table': 'pcb'
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
    mapping_details = table['mapping_details']
    source_table = mapping_details.split()[0]
    source_alias = mapping_details.split()[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            rhs = col_meta['transformation'].split('=', 1)[1].strip()
            target_column = col_meta['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
