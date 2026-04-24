from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders_bronze', 'target_alias': 'cob', 'mapping_details': 'sales_transactions_raw str', 'description': 'Create one row per transaction from sales_transactions_raw. Map: transaction_id, store_id, transaction_time as order identifiers and timestamps; sale_amount as order total amount; quantity retained as recorded units sold at transaction level.'}, {'target_schema': 'bronze', 'target_table': 'order_items_bronze', 'target_alias': 'oib', 'mapping_details': 'sales_transactions_raw str', 'description': 'Create one row per transaction item record from sales_transactions_raw. Map: transaction_id as order/transaction reference, product_id, quantity, sale_amount, store_id, transaction_time.'}, {'target_schema': 'bronze', 'target_table': 'data_quality_daily_bronze', 'target_alias': 'dqdb', 'mapping_details': 'sales_transactions_raw str', 'description': 'Store raw, row-level data quality capture for sales_transactions_raw using transaction_time as the event timestamp reference (no aggregation). Persist fields required to evaluate DQ later, including transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'cob.transaction_id = str.transaction_id', 'target_table': 'cob'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'cob.store_id = str.store_id', 'target_table': 'cob'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'cob.transaction_time = str.transaction_time', 'target_table': 'cob'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'cob.sale_amount = str.sale_amount', 'target_table': 'cob'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'null_accepted', 'transformation': 'cob.quantity = str.quantity', 'target_table': 'cob'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'oib.transaction_id = str.transaction_id', 'target_table': 'oib'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'oib.product_id = str.product_id', 'target_table': 'oib'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'null_accepted', 'transformation': 'oib.quantity = str.quantity', 'target_table': 'oib'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'oib.sale_amount = str.sale_amount', 'target_table': 'oib'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'oib.store_id = str.store_id', 'target_table': 'oib'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'oib.transaction_time = str.transaction_time', 'target_table': 'oib'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_null', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_null', 'transformation': 'dqdb.transaction_id = str.transaction_id', 'target_table': 'dqdb'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'dqdb.store_id = str.store_id', 'target_table': 'dqdb'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'null_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'null_accepted', 'transformation': 'dqdb.product_id = str.product_id', 'target_table': 'dqdb'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'null_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'null_accepted', 'transformation': 'dqdb.quantity = str.quantity', 'target_table': 'dqdb'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'null_accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'null_accepted', 'transformation': 'dqdb.sale_amount = str.sale_amount', 'target_table': 'dqdb'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'null_accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'null_accepted', 'transformation': 'dqdb.transaction_time = str.transaction_time', 'target_table': 'dqdb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    target_table = table['target_table']

    mapping_details = table['mapping_details']
    source_table = mapping_details.split()[0]
    source_alias = mapping_details.split()[1]

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == table['target_alias']:
            rhs = col_meta['transformation'].split('=', 1)[1].strip()
            target_col = col_meta['target_column']
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
