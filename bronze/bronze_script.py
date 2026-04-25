from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders_bronze', 'target_alias': 'cob', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze ingestion of sales transactions at order (transaction) level. Columns: transaction_id, store_id, sale_amount, transaction_time.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_items_bronze', 'target_alias': 'coib', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze ingestion of sales transactions at order item level (one row per transaction/product). Columns: transaction_id, product_id, quantity.'}, {'target_schema': 'bronze', 'target_table': 'etl_data_quality_results_bronze', 'target_alias': 'edqrb', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze ingestion of ETL/data quality results derived from source loads; capture basic load metadata and row-level DQ flags as ingested alongside sales transactions (no joins/aggregations).'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'cob.transaction_id = str.transaction_id', 'target_table': 'cob'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'cob.store_id = str.store_id', 'target_table': 'cob'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'cob.sale_amount = str.sale_amount', 'target_table': 'cob'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'cob.transaction_time = str.transaction_time', 'target_table': 'cob'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coib.transaction_id = str.transaction_id', 'target_table': 'coib'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'coib.product_id = str.product_id', 'target_table': 'coib'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'coib.quantity = str.quantity', 'target_table': 'coib'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'source_transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'edqrb.source_transaction_id = str.transaction_id', 'target_table': 'edqrb'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'source_transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'edqrb.source_transaction_time = str.transaction_time', 'target_table': 'edqrb'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'dq_rule_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': "edqrb.dq_rule_name = 'transaction_id_not_null'", 'target_table': 'edqrb'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'dq_pass_flag', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'edqrb.dq_pass_flag = (str.transaction_id IS NOT NULL)', 'target_table': 'edqrb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path', '')
target_path = runtime_config.get('target_path', '')
read_format = runtime_config.get('read_format', '')
write_format = runtime_config.get('write_format', '')
write_mode = runtime_config.get('write_mode', '')

for table in metadata.get('tables', []):
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    mapping_details = table.get('mapping_details', '')
    parts = mapping_details.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + '.' + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col in metadata.get('columns', []):
        if col.get('target_table') == target_alias:
            transformation = col.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else ''
            target_column = col.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()
