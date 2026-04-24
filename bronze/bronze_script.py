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
            'description': 'Bronze orders table capturing one record per sales transaction from sales_transactions_raw. Maps: transaction_id -> order_id, store_id, sale_amount -> order_total_amount, transaction_time -> order_timestamp.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_items_bronze',
            'target_alias': 'oib',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze order items table capturing line-item level details available in sales_transactions_raw (one product per transaction in this source). Maps: transaction_id -> order_id, product_id, quantity, sale_amount -> line_amount, transaction_time -> line_timestamp, store_id.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'etl_data_quality_daily_bronze',
            'target_alias': 'dqdb',
            'mapping_details': 'sales_transactions_raw str; products_raw pr; stores_raw sr',
            'description': 'Bronze ETL data quality daily events table to record daily source-level DQ checks for each raw source (sales_transactions_raw, products_raw, stores_raw). Captures per-run/per-day metrics such as record_count, null_counts, and load_timestamp derived directly from each source without joins or aggregations.'
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
            'target_column': 'line_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.line_amount = str.sale_amount',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'line_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.line_timestamp = str.transaction_time',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'oib.store_id = str.store_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'dq_event_date',
            'target_type': 'DATE',
            'target_nullable': 'not_accepted',
            'transformation': 'dqdb.dq_event_date = CAST(str.transaction_time AS DATE)',
            'target_table': 'dqdb'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'record_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'dqdb.record_id = str.transaction_id',
            'target_table': 'dqdb'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'load_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'dqdb.load_timestamp = str.transaction_time',
            'target_table': 'dqdb'
        },
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'dq_event_date',
            'target_type': 'DATE',
            'target_nullable': 'not_accepted',
            'transformation': 'dqdb.dq_event_date = CURRENT_DATE',
            'target_table': 'dqdb'
        },
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'record_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'dqdb.record_id = pr.product_id',
            'target_table': 'dqdb'
        },
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'load_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'dqdb.load_timestamp = CURRENT_TIMESTAMP',
            'target_table': 'dqdb'
        },
        {
            'source_column': "['sr.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'dq_event_date',
            'target_type': 'DATE',
            'target_nullable': 'not_accepted',
            'transformation': 'dqdb.dq_event_date = CURRENT_DATE',
            'target_table': 'dqdb'
        },
        {
            'source_column': "['sr.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'accepted',
            'target_column': 'record_id',
            'target_type': 'STRING',
            'target_nullable': 'accepted',
            'transformation': 'dqdb.record_id = sr.store_id',
            'target_table': 'dqdb'
        },
        {
            'source_column': "['sr.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'load_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'dqdb.load_timestamp = CURRENT_TIMESTAMP',
            'target_table': 'dqdb'
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
    target_table = table_meta.get('target_table')
    target_alias = table_meta.get('target_alias')
    mapping_details = table_meta.get('mapping_details')

    source_mappings = []
    if mapping_details:
        for part in mapping_details.split(';'):
            part = part.strip()
            if part:
                tokens = part.split()
                if len(tokens) >= 2:
                    source_mappings.append((tokens[0].strip(), tokens[1].strip()))

    if not source_mappings:
        continue

    source_table, source_alias = source_mappings[0]

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation')
            target_col = col_meta.get('target_column')
            if transformation and target_col and '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
                transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
