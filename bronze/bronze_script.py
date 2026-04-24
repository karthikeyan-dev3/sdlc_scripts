from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'orders_bronze', 'target_alias': 'ob', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze orders table mapped 1:1 from sales_transactions_raw. Columns: transaction_id->order_id, store_id, transaction_time->order_timestamp, sale_amount->order_total_amount.'}, {'target_schema': 'bronze', 'target_table': 'order_items_bronze', 'target_alias': 'oib', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze order items table mapped 1:1 from sales_transactions_raw. Columns: transaction_id->order_id, product_id, quantity, sale_amount->line_amount.'}, {'target_schema': 'bronze', 'target_table': 'order_statuses_bronze', 'target_alias': 'osb', 'mapping_details': 'sales_transactions_raw str', 'description': "Bronze order statuses table mapped 1:1 from sales_transactions_raw with status derived as a constant. Columns: transaction_id->order_id, status='COMPLETED', status_timestamp=transaction_time."}, {'target_schema': 'bronze', 'target_table': 'load_lineage_bronze', 'target_alias': 'llb', 'mapping_details': 'sales_transactions_raw str', 'description': "Bronze load lineage table capturing source record identifiers and load metadata at ingestion. Columns: transaction_id as source_record_id, source_table='sales_transactions_raw', source_event_time=transaction_time."}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'ob.order_id = str.transaction_id', 'target_table': 'ob'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'ob.store_id = str.store_id', 'target_table': 'ob'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'order_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'ob.order_timestamp = str.transaction_time', 'target_table': 'ob'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'order_total_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'ob.order_total_amount = str.sale_amount', 'target_table': 'ob'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'oib.order_id = str.transaction_id', 'target_table': 'oib'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'oib.product_id = str.product_id', 'target_table': 'oib'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'oib.quantity = str.quantity', 'target_table': 'oib'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'line_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'oib.line_amount = str.sale_amount', 'target_table': 'oib'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'osb.order_id = str.transaction_id', 'target_table': 'osb'}, {'source_column': '[]', 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'status', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': "osb.status = 'COMPLETED'", 'target_table': 'osb'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'status_timestamp', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'osb.status_timestamp = str.transaction_time', 'target_table': 'osb'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'source_record_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'llb.source_record_id = str.transaction_id', 'target_table': 'llb'}, {'source_column': '[]', 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'source_table', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': "llb.source_table = 'sales_transactions_raw'", 'target_table': 'llb'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'source_event_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'llb.source_event_time = str.transaction_time', 'target_table': 'llb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

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
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col in metadata.get('columns', []):
        if col.get('target_table') == target_alias:
            transformation = col.get('transformation', '')
            target_column = col.get('target_column')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()