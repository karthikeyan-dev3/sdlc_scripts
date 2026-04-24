from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders', 'target_alias': 'co', 'mapping_details': 'sales_transactions_raw str', 'description': 'Ingest customer order-level records from sales_transactions_raw at transaction granularity (one row per transaction_id). Map: transaction_id->order_id, store_id->store_id, transaction_time->order_timestamp, sale_amount->order_total_amount.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_items', 'target_alias': 'coi', 'mapping_details': 'sales_transactions_raw str', 'description': 'Ingest customer order item records from sales_transactions_raw at transaction-product granularity as available (one row per transaction_id and product_id). Map: transaction_id->order_id, product_id->product_id, quantity->quantity, sale_amount->line_amount.'}, {'target_schema': 'bronze', 'target_table': 'ingestion_run_audit', 'target_alias': 'ira', 'mapping_details': 'sales_transactions_raw str; products_raw pr; stores_raw sr', 'description': 'Capture ingestion run audit metadata for each raw source ingestion (sales_transactions_raw, products_raw, stores_raw). Store batch/run identifiers, source name, ingestion start/end timestamps, record counts, and load status as provided by the ingestion framework.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'co.order_id = str.transaction_id', 'target_table': 'co'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'co.store_id = str.store_id', 'target_table': 'co'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'order_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'co.order_timestamp = str.transaction_time', 'target_table': 'co'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_accepted', 'target_column': 'order_total_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_accepted', 'transformation': 'co.order_total_amount = str.sale_amount', 'target_table': 'co'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coi.order_id = str.transaction_id', 'target_table': 'coi'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coi.product_id = str.product_id', 'target_table': 'coi'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'coi.quantity = str.quantity', 'target_table': 'coi'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'not_accepted', 'target_column': 'line_amount', 'target_type': 'DECIMAL', 'target_nullable': 'not_accepted', 'transformation': 'coi.line_amount = str.sale_amount', 'target_table': 'coi'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'run_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'ira.run_id = ingestion_framework.run_id', 'target_table': 'ira'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'source_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'ira.source_name = ingestion_framework.source_name', 'target_table': 'ira'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'ingestion_start_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'ira.ingestion_start_timestamp = ingestion_framework.ingestion_start_timestamp', 'target_table': 'ira'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'ingestion_end_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'ira.ingestion_end_timestamp = ingestion_framework.ingestion_end_timestamp', 'target_table': 'ira'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'record_count', 'target_type': 'BIGINT', 'target_nullable': 'not_accepted', 'transformation': 'ira.record_count = ingestion_framework.record_count', 'target_table': 'ira'}, {'source_column': '[]', 'source_type': 'nan', 'source_nullable': 'not_accepted', 'target_column': 'load_status', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'ira.load_status = ingestion_framework.load_status', 'target_table': 'ira'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table_meta in metadata.get('tables', []):
    target_table = table_meta.get('target_table')
    target_alias = table_meta.get('target_alias')
    mapping_details = table_meta.get('mapping_details', '')

    first_mapping = mapping_details.split(';')[0].strip() if mapping_details else ''
    source_table = first_mapping.split()[0].strip() if first_mapping else ''
    source_alias = first_mapping.split()[1].strip() if len(first_mapping.split()) > 1 else ''

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + '.' + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            target_column = col_meta.get('target_column')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else ''
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()
