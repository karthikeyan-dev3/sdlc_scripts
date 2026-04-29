from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

# Configurations
base_path = 's3://sdlc-agent-bucket/engineering-agent/src/'
target_path = 's3://sdlc-agent-bucket/engineering-agent/bronze/'
read_format = 'csv'
write_format = 'csv'
write_mode = 'overwrite'

# Tables Metadata
tables_metadata = [
    {
        'target_table': 'sales_event_bronze',
        'source_table': 'POS sales_event',
        'source_alias': 'seb'
    },
    {
        'target_table': 'payment_event_bronze',
        'source_table': 'PAYMENT_GATEWAY payment_event',
        'source_alias': 'peb'
    },
    {
        'target_table': 'inventory_event_bronze',
        'source_table': 'INVENTORY_SYSTEM inventory_event',
        'source_alias': 'ieb'
    },
    {
        'target_table': 'footfall_event_bronze',
        'source_table': 'SENSOR footfall_event',
        'source_alias': 'feb'
    },
]

# Columns Metadata
columns_metadata = [
    {'transformation': 'seb.event_id = seb.event_id', 'target_alias': 'seb', 'target_column': 'event_id'},
    {'transformation': 'seb.event_type = seb.event_type', 'target_alias': 'seb', 'target_column': 'event_type'},
    {'transformation': 'seb.source_system = seb.source_system', 'target_alias': 'seb', 'target_column': 'source_system'},
    {'transformation': 'seb.event_timestamp = seb.event_timestamp', 'target_alias': 'seb', 'target_column': 'event_timestamp'},
    {'transformation': 'seb.ingestion_timestamp = seb.ingestion_timestamp', 'target_alias': 'seb', 'target_column': 'ingestion_timestamp'},
    {'transformation': 'seb.batch_id = seb.batch_id', 'target_alias': 'seb', 'target_column': 'batch_id'},
    {'transformation': 'seb.is_deleted = seb.is_deleted', 'target_alias': 'seb', 'target_column': 'is_deleted'},
    {'transformation': 'seb.transaction_id = seb.transaction_id', 'target_alias': 'seb', 'target_column': 'transaction_id'},
    {'transformation': 'seb.order_id = seb.order_id', 'target_alias': 'seb', 'target_column': 'order_id'},
    {'transformation': 'seb.store_id = seb.store_id', 'target_alias': 'seb', 'target_column': 'store_id'},
    {'transformation': 'seb.terminal_id = seb.terminal_id', 'target_alias': 'seb', 'target_column': 'terminal_id'},
    {'transformation': 'seb.cashier_id = seb.cashier_id', 'target_alias': 'seb', 'target_column': 'cashier_id'},
    {'transformation': 'seb.product_id = seb.product_id', 'target_alias': 'seb', 'target_column': 'product_id'},
    {'transformation': 'seb.product_name = seb.product_name', 'target_alias': 'seb', 'target_column': 'product_name'},
    {'transformation': 'seb.category = seb.category', 'target_alias': 'seb', 'target_column': 'category'},
    {'transformation': 'seb.sub_category = seb.sub_category', 'target_alias': 'seb', 'target_column': 'sub_category'},
    {'transformation': 'seb.quantity = seb.quantity', 'target_alias': 'seb', 'target_column': 'quantity'},
    {'transformation': 'seb.unit_price = seb.unit_price', 'target_alias': 'seb', 'target_column': 'unit_price'},
    {'transformation': 'seb.discount = seb.discount', 'target_alias': 'seb', 'target_column': 'discount'},
    {'transformation': 'seb.total_amount = seb.total_amount', 'target_alias': 'seb', 'target_column': 'total_amount'},
    {'transformation': 'seb.payment_id = seb.payment_id', 'target_alias': 'seb', 'target_column': 'payment_id'},
    {'transformation': 'seb.event_action = seb.event_action', 'target_alias': 'seb', 'target_column': 'event_action'},
    {'transformation': 'peb.event_id = peb.event_id', 'target_alias': 'peb', 'target_column': 'event_id'},
    {'transformation': 'peb.event_type = peb.event_type', 'target_alias': 'peb', 'target_column': 'event_type'},
    {'transformation': 'peb.source_system = peb.source_system', 'target_alias': 'peb', 'target_column': 'source_system'},
    {'transformation': 'peb.event_timestamp = peb.event_timestamp', 'target_alias': 'peb', 'target_column': 'event_timestamp'},
    {'transformation': 'peb.ingestion_timestamp = peb.ingestion_timestamp', 'target_alias': 'peb', 'target_column': 'ingestion_timestamp'},
    {'transformation': 'peb.batch_id = peb.batch_id', 'target_alias': 'peb', 'target_column': 'batch_id'},
    {'transformation': 'peb.is_deleted = peb.is_deleted', 'target_alias': 'peb', 'target_column': 'is_deleted'},
    {'transformation': 'peb.payment_id = peb.payment_id', 'target_alias': 'peb', 'target_column': 'payment_id'},
    {'transformation': 'peb.transaction_id = peb.transaction_id', 'target_alias': 'peb', 'target_column': 'transaction_id'},
    {'transformation': 'peb.payment_mode = peb.payment_mode', 'target_alias': 'peb', 'target_column': 'payment_mode'},
    {'transformation': 'peb.provider = peb.provider', 'target_alias': 'peb', 'target_column': 'provider'},
    {'transformation': 'peb.amount = peb.amount', 'target_alias': 'peb', 'target_column': 'amount'},
    {'transformation': 'peb.currency = peb.currency', 'target_alias': 'peb', 'target_column': 'currency'},
    {'transformation': 'peb.payment_status = peb.payment_status', 'target_alias': 'peb', 'target_column': 'payment_status'},
    {'transformation': 'ieb.event_id = ieb.event_id', 'target_alias': 'ieb', 'target_column': 'event_id'},
    {'transformation': 'ieb.event_type = ieb.event_type', 'target_alias': 'ieb', 'target_column': 'event_type'},
    {'transformation': 'ieb.source_system = ieb.source_system', 'target_alias': 'ieb', 'target_column': 'source_system'},
    {'transformation': 'ieb.event_timestamp = ieb.event_timestamp', 'target_alias': 'ieb', 'target_column': 'event_timestamp'},
    {'transformation': 'ieb.ingestion_timestamp = ieb.ingestion_timestamp', 'target_alias': 'ieb', 'target_column': 'ingestion_timestamp'},
    {'transformation': 'ieb.batch_id = ieb.batch_id', 'target_alias': 'ieb', 'target_column': 'batch_id'},
    {'transformation': 'ieb.is_deleted = ieb.is_deleted', 'target_alias': 'ieb', 'target_column': 'is_deleted'},
    {'transformation': 'ieb.inventory_event_id = ieb.inventory_event_id', 'target_alias': 'ieb', 'target_column': 'inventory_event_id'},
    {'transformation': 'ieb.product_id = ieb.product_id', 'target_alias': 'ieb', 'target_column': 'product_id'},
    {'transformation': 'ieb.store_id = ieb.store_id', 'target_alias': 'ieb', 'target_column': 'store_id'},
    {'transformation': 'ieb.warehouse_id = ieb.warehouse_id', 'target_alias': 'ieb', 'target_column': 'warehouse_id'},
    {'transformation': 'ieb.change_type = ieb.change_type', 'target_alias': 'ieb', 'target_column': 'change_type'},
    {'transformation': 'ieb.quantity_changed = ieb.quantity_changed', 'target_alias': 'ieb', 'target_column': 'quantity_changed'},
    {'transformation': 'ieb.current_stock = ieb.current_stock', 'target_alias': 'ieb', 'target_column': 'current_stock'},
    {'transformation': 'feb.event_id = feb.event_id', 'target_alias': 'feb', 'target_column': 'event_id'},
    {'transformation': 'feb.event_type = feb.event_type', 'target_alias': 'feb', 'target_column': 'event_type'},
    {'transformation': 'feb.source_system = feb.source_system', 'target_alias': 'feb', 'target_column': 'source_system'},
    {'transformation': 'feb.event_timestamp = feb.event_timestamp', 'target_alias': 'feb', 'target_column': 'event_timestamp'},
    {'transformation': 'feb.ingestion_timestamp = feb.ingestion_timestamp', 'target_alias': 'feb', 'target_column': 'ingestion_timestamp'},
    {'transformation': 'feb.batch_id = feb.batch_id', 'target_alias': 'feb', 'target_column': 'batch_id'},
    {'transformation': 'feb.is_deleted = feb.is_deleted', 'target_alias': 'feb', 'target_column': 'is_deleted'},
    {'transformation': 'feb.footfall_event_id = feb.footfall_event_id', 'target_alias': 'feb', 'target_column': 'footfall_event_id'},
    {'transformation': 'feb.store_id = feb.store_id', 'target_alias': 'feb', 'target_column': 'store_id'},
    {'transformation': 'feb.entry_count = feb.entry_count', 'target_alias': 'feb', 'target_column': 'entry_count'},
    {'transformation': 'feb.exit_count = feb.exit_count', 'target_alias': 'feb', 'target_column': 'exit_count'},
    {'transformation': 'feb.sensor_id = feb.sensor_id', 'target_alias': 'feb', 'target_column': 'sensor_id'},
]

# Process Each Table
for table_meta in tables_metadata:
    source_alias = table_meta['source_alias']
    source_table = table_meta['source_table']
    target_table = table_meta['target_table']

    # Read Data
    df = spark.read.format(read_format)\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(f"{base_path}{source_table}.{read_format}")

    # Apply Alias
    df = df.alias(source_alias)

    # Column Transformation
    transformations = [col_meta['transformation'].split(' = ')[1] + f" as {col_meta['target_column']}"
                       for col_meta in columns_metadata if col_meta['target_alias'] == source_alias]

    # Select
    df = df.selectExpr(*transformations)

    # Write Data
    df.write.mode(write_mode).format(write_format)\
        .option("header", "true")\
        .save(f"{target_path}{target_table}.{write_format}")

job.commit()