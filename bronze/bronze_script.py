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
            'description': 'Bronze-level capture of order headers from raw sales transactions. Columns retained as-is: transaction_id (order_id), store_id, sale_amount, transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_order_items_bronze',
            'target_alias': 'coib',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze-level capture of order line items from raw sales transactions. Columns retained as-is: transaction_id (order_id), product_id, quantity, sale_amount, transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'etl_order_data_quality_daily_bronze',
            'target_alias': 'eodqdb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze-level daily ETL data quality inputs derived directly from raw sales transactions without aggregation. Columns retained as-is for downstream DQ computations: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
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
            'transformation': 'cob.transaction_id = str.transaction_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'cob.store_id = str.store_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'cob.sale_amount = str.sale_amount',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'cob.transaction_time = str.transaction_time',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'coib.transaction_id = str.transaction_id',
            'target_table': 'coib'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'coib.product_id = str.product_id',
            'target_table': 'coib'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'coib.quantity = str.quantity',
            'target_table': 'coib'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'coib.sale_amount = str.sale_amount',
            'target_table': 'coib'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'coib.transaction_time = str.transaction_time',
            'target_table': 'coib'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'eodqdb.transaction_id = str.transaction_id',
            'target_table': 'eodqdb'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'eodqdb.store_id = str.store_id',
            'target_table': 'eodqdb'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'eodqdb.product_id = str.product_id',
            'target_table': 'eodqdb'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'accepted',
            'transformation': 'eodqdb.quantity = str.quantity',
            'target_table': 'eodqdb'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'accepted',
            'transformation': 'eodqdb.sale_amount = str.sale_amount',
            'target_table': 'eodqdb'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'accepted',
            'transformation': 'eodqdb.transaction_time = str.transaction_time',
            'target_table': 'eodqdb'
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
    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col in metadata['columns']:
        if col['target_table'] == target_alias:
            rhs = col['transformation'].split('=', 1)[1].strip()
            target_col = col['target_column']
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
