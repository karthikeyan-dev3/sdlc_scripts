from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'pos_sales_event_bronze', 'target_alias': 'pseb', 'mapping_details': "source: POS events where event_metadata.event_type = 'sales'; map event_metadata.* and sales_event.* 1:1 into columns", 'description': 'Bronze raw POS sales events capturing transaction/order/store/terminal/cashier/product fields, quantities, pricing/discount/total_amount, payment_id and event_action, along with event metadata (event_id, timestamps, batch_id, is_deleted). No deduplication, standardization, joins, or aggregations applied.'}, {'target_schema': 'bronze', 'target_table': 'payment_gateway_event_bronze', 'target_alias': 'pgeb', 'mapping_details': "source: PAYMENT_GATEWAY events where event_metadata.event_type = 'payment'; map event_metadata.* and payment_event.* 1:1 into columns", 'description': 'Bronze raw payment events capturing payment_id, transaction_id, payment_mode, provider, amount, currency, payment_status, along with event metadata (event_id, timestamps, batch_id, is_deleted). No currency normalization, joins, or aggregations applied.'}], 'columns': [{'source_column': "['pseb.sales_event.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.transaction_id = pseb.sales_event.transaction_id', 'target_table': 'pseb'}, {'source_column': "['pseb.event_metadata.event_timestamp']", 'source_type': 'TIMESTAMP', 'source_nullable': 'not_accepted', 'target_column': 'transaction_date', 'target_type': 'TIMESTAMP', 'target_nullable': 'not_accepted', 'transformation': 'pseb.event_metadata.event_timestamp = pseb.event_metadata.event_timestamp', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.total_amount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'not_accepted', 'target_column': 'sales_amount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.total_amount = pseb.sales_event.total_amount', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.quantity']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'quantity_sold', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.quantity = pseb.sales_event.quantity', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.product_id = pseb.sales_event.product_id', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.product_name']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.product_name = pseb.sales_event.product_name', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.category']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_category', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.category = pseb.sales_event.category', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.store_id = pseb.sales_event.store_id', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.product_id = pseb.sales_event.product_id', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.product_name']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.product_name = pseb.sales_event.product_name', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.category']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_category', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.category = pseb.sales_event.category', 'target_table': 'pseb'}, {'source_column': "['pseb.sales_event.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.sales_event.store_id = pseb.sales_event.store_id', 'target_table': 'pseb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    source_table = table['mapping_details'].split(': ')[1].split(' ')[0]
    source_alias = table['target_alias']
    target_table = table['target_table']
    
    df = spark.read.format(read_format)
    if read_format == 'csv':
        df = df.option("header", "true").option("inferSchema", "true")
    df = df.load(base_path + source_table + '.' + read_format)
    df = df.alias(source_alias)

    transformations = []
    for column in metadata['columns']:
        if column['target_table'] == source_alias:
            rhs = column['transformation'].split('=')[1].strip()
            target_column = column['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    if write_format == 'csv':
        df.write.mode(write_mode).option("header", "true").format(write_format).save(target_path + target_table + '.' + write_format)
    else:
        df.write.mode(write_mode).format(write_format).save(target_path + target_table + '.' + write_format)

job.commit()