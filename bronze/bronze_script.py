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
            'description': 'Bronze orders table mapped 1:1 from sales_transactions_raw. Columns: transaction_id (order_id), store_id, sale_amount (order_total_amount), transaction_time (order_timestamp).'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_items_bronze',
            'target_alias': 'oib',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze order items table mapped 1:1 from sales_transactions_raw. Columns: transaction_id (order_id), product_id, quantity, sale_amount (line_total_amount), transaction_time.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'daily_etl_run_metrics_bronze',
            'target_alias': 'dermb',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze ETL run metrics staging table sourced from sales_transactions_raw; intended to capture ingestion/run metadata and raw row-level audit fields derived during ingestion (e.g., load_date, ingestion_timestamp, source_row_hash) without aggregation.'
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
            'transformation': 'ob.order_id = str.transaction_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'ob.store_id = str.store_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'order_total_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'ob.order_total_amount = str.sale_amount',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'order_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'ob.order_timestamp = str.transaction_time',
            'target_table': 'ob'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.order_id = str.transaction_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.product_id = str.product_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.quantity = str.quantity',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'line_total_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.line_total_amount = str.sale_amount',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.transaction_time = str.transaction_time',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'source_transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'dermb.source_transaction_id = str.transaction_id',
            'target_table': 'dermb'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'source_store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'dermb.source_store_id = str.store_id',
            'target_table': 'dermb'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'source_product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'dermb.source_product_id = str.product_id',
            'target_table': 'dermb'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'source_quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'dermb.source_quantity = str.quantity',
            'target_table': 'dermb'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'source_sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'dermb.source_sale_amount = str.sale_amount',
            'target_table': 'dermb'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'source_transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'dermb.source_transaction_time = str.transaction_time',
            'target_table': 'dermb'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'load_date',
            'target_type': 'DATE',
            'target_nullable': 'not_accepted',
            'transformation': 'dermb.load_date = CAST(str.transaction_time AS DATE)',
            'target_table': 'dermb'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'ingestion_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'dermb.ingestion_timestamp = str.transaction_time',
            'target_table': 'dermb'
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
write_mode = metadata['runtime_config']['write_mode']
base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']

for table in metadata['tables']:
    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            rhs = col_meta['transformation'].split('=', 1)[1].strip()
            target_col = col_meta['target_column']
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
