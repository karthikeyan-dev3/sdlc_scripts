from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'pos_sales_event_bronze', 'target_alias': 'pseb', 'mapping_details': 'POS.sales_event se', 'description': 'Raw POS sales events at line-item level. One row per sales_event message. Includes transaction_id, order_id, store_id, terminal_id, cashier_id, product_id, product_name, category, sub_category, quantity, unit_price, discount, total_amount, payment_id, event_action.'}, {'target_schema': 'bronze', 'target_table': 'payment_gateway_event_bronze', 'target_alias': 'pgeb', 'mapping_details': 'PAYMENT_GATEWAY.payment_event pe', 'description': 'Raw payment gateway events. One row per payment_event message. Includes payment_id, transaction_id, payment_mode, provider, amount, currency, payment_status.'}], 'columns': [{'source_column': "['pseb.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.transaction_id = pseb.transaction_id', 'target_table': 'pseb'}, {'source_column': "['pseb.order_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'order_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.order_id = pseb.order_id', 'target_table': 'pseb'}, {'source_column': "['pseb.store_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'store_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.store_id = pseb.store_id', 'target_table': 'pseb'}, {'source_column': "['pseb.terminal_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'terminal_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.terminal_id = pseb.terminal_id', 'target_table': 'pseb'}, {'source_column': "['pseb.cashier_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'cashier_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.cashier_id = pseb.cashier_id', 'target_table': 'pseb'}, {'source_column': "['pseb.product_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'product_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pseb.product_id = pseb.product_id', 'target_table': 'pseb'}, {'source_column': "['pseb.product_name']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'product_name', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.product_name = pseb.product_name', 'target_table': 'pseb'}, {'source_column': "['pseb.category']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'category', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.category = pseb.category', 'target_table': 'pseb'}, {'source_column': "['pseb.sub_category']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'sub_category', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.sub_category = pseb.sub_category', 'target_table': 'pseb'}, {'source_column': "['pseb.quantity']", 'source_type': 'INT', 'source_nullable': 'not_accepted', 'target_column': 'quantity', 'target_type': 'INT', 'target_nullable': 'not_accepted', 'transformation': 'pseb.quantity = pseb.quantity', 'target_table': 'pseb'}, {'source_column': "['pseb.unit_price']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'not_accepted', 'target_column': 'unit_price', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'not_accepted', 'transformation': 'pseb.unit_price = pseb.unit_price', 'target_table': 'pseb'}, {'source_column': "['pseb.discount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'accepted', 'target_column': 'discount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'accepted', 'transformation': 'pseb.discount = pseb.discount', 'target_table': 'pseb'}, {'source_column': "['pseb.total_amount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'not_accepted', 'target_column': 'total_amount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'not_accepted', 'transformation': 'pseb.total_amount = pseb.total_amount', 'target_table': 'pseb'}, {'source_column': "['pseb.payment_id']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'payment_id', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.payment_id = pseb.payment_id', 'target_table': 'pseb'}, {'source_column': "['pseb.event_action']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'event_action', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pseb.event_action = pseb.event_action', 'target_table': 'pseb'}, {'source_column': "['pgeb.payment_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'payment_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pgeb.payment_id = pgeb.payment_id', 'target_table': 'pgeb'}, {'source_column': "['pgeb.transaction_id']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'transaction_id', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pgeb.transaction_id = pgeb.transaction_id', 'target_table': 'pgeb'}, {'source_column': "['pgeb.payment_mode']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'payment_mode', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pgeb.payment_mode = pgeb.payment_mode', 'target_table': 'pgeb'}, {'source_column': "['pgeb.provider']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'provider', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pgeb.provider = pgeb.provider', 'target_table': 'pgeb'}, {'source_column': "['pgeb.amount']", 'source_type': 'DECIMAL(18,2)', 'source_nullable': 'not_accepted', 'target_column': 'amount', 'target_type': 'DECIMAL(18,2)', 'target_nullable': 'not_accepted', 'transformation': 'pgeb.amount = pgeb.amount', 'target_table': 'pgeb'}, {'source_column': "['pgeb.currency']", 'source_type': 'STRING', 'source_nullable': 'not_accepted', 'target_column': 'currency', 'target_type': 'STRING', 'target_nullable': 'not_accepted', 'transformation': 'pgeb.currency = pgeb.currency', 'target_table': 'pgeb'}, {'source_column': "['pgeb.payment_status']", 'source_type': 'STRING', 'source_nullable': 'accepted', 'target_column': 'payment_status', 'target_type': 'STRING', 'target_nullable': 'accepted', 'transformation': 'pgeb.payment_status = pgeb.payment_status', 'target_table': 'pgeb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    source_table, source_alias = table['mapping_details'].split()
    target_table = table['target_table']
    target_alias = table['target_alias']
    
    df = spark.read.format(read_format)
    if read_format == 'csv':
        df = df.option("header", "true").option("inferSchema", "true")
    df = df.load(base_path + source_table + '.' + read_format)
    
    df = df.alias(source_alias)
    
    transformations = []
    for column in metadata['columns']:
        if column['target_table'] == target_alias:
            rhs = column['transformation'].split('=')[1].strip()
            target_column = column['target_column']
            transformations.append(f"{rhs} as {target_column}")
    
    df = df.selectExpr(*transformations)
    
    df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        df = df.option("header", "true")
    df.save(target_path + target_table + '.' + write_format)

job.commit()