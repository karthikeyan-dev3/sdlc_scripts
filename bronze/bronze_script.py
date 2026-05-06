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
            'target_table': 'bronze_products_raw',
            'target_alias': 'bpr',
            'mapping_details': 'products_raw pr',
            'description': 'Raw ingestion of Product Master data exactly as received from source products_raw (product_id, product_name, category, brand, price, is_active) with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'bronze_stores_raw',
            'target_alias': 'bsr',
            'mapping_details': 'stores_raw sr',
            'description': 'Raw ingestion of Store Master data exactly as received from source stores_raw (store_id, store_name, city, state, store_type, open_date) with no transformations, joins, or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'bronze_sales_transactions_raw',
            'target_alias': 'bstr',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Raw incremental ingestion of Sales Transactions data exactly as received from source sales_transactions_raw (transaction_id, store_id, product_id, quantity, sale_amount, transaction_time) with no transformations, joins, or aggregations; preserves immutable history.'
        }
    ],
    'columns': [
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'bpr.product_id = pr.product_id',
            'target_table': 'bpr'
        },
        {
            'source_column': "['pr.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'bpr.product_name = pr.product_name',
            'target_table': 'bpr'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'bpr.category = pr.category',
            'target_table': 'bpr'
        },
        {
            'source_column': "['pr.brand']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'bpr.brand = pr.brand',
            'target_table': 'bpr'
        },
        {
            'source_column': "['pr.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'bpr.price = pr.price',
            'target_table': 'bpr'
        },
        {
            'source_column': "['pr.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'accepted',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'accepted',
            'transformation': 'bpr.is_active = pr.is_active',
            'target_table': 'bpr'
        },
        {
            'source_column': "['sr.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'bsr.store_id = sr.store_id',
            'target_table': 'bsr'
        },
        {
            'source_column': "['sr.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'bsr.store_name = sr.store_name',
            'target_table': 'bsr'
        },
        {
            'source_column': "['sr.city']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'bsr.city = sr.city',
            'target_table': 'bsr'
        },
        {
            'source_column': "['sr.state']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'bsr.state = sr.state',
            'target_table': 'bsr'
        },
        {
            'source_column': "['sr.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'bsr.store_type = sr.store_type',
            'target_table': 'bsr'
        },
        {
            'source_column': "['sr.open_date']",
            'source_type': 'DATE',
            'source_nullable': 'accepted',
            'target_column': 'open_date',
            'target_type': 'DATE',
            'target_nullable': 'accepted',
            'transformation': 'bsr.open_date = sr.open_date',
            'target_table': 'bsr'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'bstr.transaction_id = str.transaction_id',
            'target_table': 'bstr'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'bstr.store_id = str.store_id',
            'target_table': 'bstr'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'bstr.product_id = str.product_id',
            'target_table': 'bstr'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'bstr.quantity = str.quantity',
            'target_table': 'bstr'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'bstr.sale_amount = str.sale_amount',
            'target_table': 'bstr'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'bstr.transaction_time = str.transaction_time',
            'target_table': 'bstr'
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

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    target_table = table['target_table']
    target_alias = table['target_alias']

    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
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

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
