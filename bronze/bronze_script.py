from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders_bronze', 'target_alias': 'cob', 'mapping_details': 'sales_transactions_raw str', 'description': 'Ingest raw sales transactions at the order/transaction grain. Map: transaction_id, store_id, transaction_time, sale_amount from sales_transactions_raw.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_line_items_bronze', 'target_alias': 'colib', 'mapping_details': 'sales_transactions_raw str', 'description': 'Ingest raw sales transaction line-item fields available in source. Map: transaction_id, product_id, quantity from sales_transactions_raw.'}, {'target_schema': 'bronze', 'target_table': 'etl_batch_quality_metrics_bronze', 'target_alias': 'ebqmb', 'mapping_details': 'sales_transactions_raw str', 'description': 'Capture batch-level quality/ingestion metadata derived during load of sales_transactions_raw (e.g., batch_id, load_timestamp, source_row_count, rejected_row_count, null/duplicate counts) without joining or aggregating across source tables.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'cob.transaction_id = str.transaction_id', 'target_table': 'cob'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'cob.store_id = str.store_id', 'target_table': 'cob'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'cob.transaction_time = str.transaction_time', 'target_table': 'cob'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'cob.sale_amount = str.sale_amount', 'target_table': 'cob'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'colib.transaction_id = str.transaction_id', 'target_table': 'colib'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'colib.product_id = str.product_id', 'target_table': 'colib'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'colib.quantity = str.quantity', 'target_table': 'colib'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'load_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'ebqmb.load_timestamp = CURRENT_TIMESTAMP()', 'target_table': 'ebqmb'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'source_row_count', 'target_type': 'BIGINT', 'target_nullable': 'not_accepted', 'transformation': 'ebqmb.source_row_count = 1', 'target_table': 'ebqmb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

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

    df = reader.load(base_path + source_table + "." + read_format)
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

    writer.save(target_path + target_table + "." + write_format)

job.commit()
