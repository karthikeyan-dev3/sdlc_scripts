from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'customer_orders', 'target_alias': 'co', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze-level raw capture of sales transactions at the order/transaction grain. Map 1:1 from sales_transactions_raw: transaction_id, store_id, transaction_time, sale_amount, quantity, product_id.'}, {'target_schema': 'bronze', 'target_table': 'customer_order_items', 'target_alias': 'coi', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze-level raw capture of order line items at the transaction-product grain as provided by source. Map 1:1 from sales_transactions_raw: transaction_id, product_id, quantity, sale_amount, store_id, transaction_time.'}, {'target_schema': 'bronze', 'target_table': 'order_data_quality_summary', 'target_alias': 'odqs', 'mapping_details': 'sales_transactions_raw str', 'description': 'Bronze-level data quality capture for orders; store row-level raw flags/fields derived without aggregation from sales_transactions_raw (e.g., transaction_id plus checks for null/invalid store_id, product_id, quantity, sale_amount, transaction_time). No joins or aggregations.'}], 'columns': [{'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'co.transaction_id = str.transaction_id', 'target_table': 'co'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'co.store_id = str.store_id', 'target_table': 'co'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'co.transaction_time = str.transaction_time', 'target_table': 'co'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'co.sale_amount = str.sale_amount', 'target_table': 'co'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'co.quantity = str.quantity', 'target_table': 'co'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'co.product_id = str.product_id', 'target_table': 'co'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'coi.transaction_id = str.transaction_id', 'target_table': 'coi'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'coi.product_id = str.product_id', 'target_table': 'coi'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'accepted', 'transformation': 'coi.quantity = str.quantity', 'target_table': 'coi'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount', 'target_type': 'DECIMAL', 'target_nullable': 'accepted', 'transformation': 'coi.sale_amount = str.sale_amount', 'target_table': 'coi'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'coi.store_id = str.store_id', 'target_table': 'coi'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time', 'target_type': 'TIMESTAMP', 'target_nullable': 'accepted', 'transformation': 'coi.transaction_time = str.transaction_time', 'target_table': 'coi'}, {'source_column': "['str.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'odqs.transaction_id = str.transaction_id', 'target_table': 'odqs'}, {'source_column': "['str.store_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'store_id_is_null', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'odqs.store_id_is_null = (str.store_id IS NULL)', 'target_table': 'odqs'}, {'source_column': "['str.product_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_id_is_null', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'odqs.product_id_is_null = (str.product_id IS NULL)', 'target_table': 'odqs'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity_is_null', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'odqs.quantity_is_null = (str.quantity IS NULL)', 'target_table': 'odqs'}, {'source_column': "['str.quantity']", 'source_type': 'INT', 'source_nullable': 'accepted', 'target_column': 'quantity_is_invalid', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'odqs.quantity_is_invalid = (str.quantity IS NOT NULL AND str.quantity <= 0)', 'target_table': 'odqs'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount_is_null', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'odqs.sale_amount_is_null = (str.sale_amount IS NULL)', 'target_table': 'odqs'}, {'source_column': "['str.sale_amount']", 'source_type': 'DECIMAL', 'source_nullable': 'accepted', 'target_column': 'sale_amount_is_invalid', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'odqs.sale_amount_is_invalid = (str.sale_amount IS NOT NULL AND str.sale_amount < 0)', 'target_table': 'odqs'}, {'source_column': "['str.transaction_time']", 'source_type': 'TIMESTAMP', 'source_nullable': 'accepted', 'target_column': 'transaction_time_is_null', 'target_type': 'BOOLEAN', 'target_nullable': 'not_accepted', 'transformation': 'odqs.transaction_time_is_null = (str.transaction_time IS NULL)', 'target_table': 'odqs'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    target_table = table['target_table']
    target_alias = table['target_alias']

    mapping_details = table['mapping_details']
    source_table, source_alias = mapping_details.split()

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(f"{base_path}{source_table}.{read_format}")
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
        writer = writer.option('header', 'true')

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()
