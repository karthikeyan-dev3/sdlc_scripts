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
            'target_table': 'customer_orders_bronze',
            'target_alias': 'cob',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze-level customer_orders derived directly from sales_transactions_raw. Map: cob.order_id = str.transaction_id, cob.store_id = str.store_id, cob.order_time = str.transaction_time, cob.total_amount = str.sale_amount, cob.total_quantity = str.quantity.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_order_items_bronze',
            'target_alias': 'coib',
            'mapping_details': 'sales_transactions_raw str',
            'description': 'Bronze-level customer_order_items derived directly from sales_transactions_raw at transaction granularity. Map: coib.order_id = str.transaction_id, coib.product_id = str.product_id, coib.quantity = str.quantity, coib.line_amount = str.sale_amount.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw pr',
            'description': 'Bronze-level products loaded directly from products_raw. Map: pb.product_id = pr.product_id, pb.product_name = pr.product_name, pb.category = pr.category, pb.brand = pr.brand, pb.price = pr.price, pb.is_active = pr.is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'etl_load_audit_orders_bronze',
            'target_alias': 'elaob',
            'mapping_details': 'sales_transactions_raw str',
            'description': "Bronze-level ETL load audit for orders sourced from sales_transactions_raw. Capture minimal audit attributes such as source_system_table = 'sales_transactions_raw', load_watermark_time = max observed str.transaction_time at extraction (no aggregation performed in bronze), and row-level reference key = str.transaction_id."
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
            'transformation': 'cob.order_id = str.transaction_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.store_id = str.store_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'order_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.order_time = str.transaction_time',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'total_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.total_amount = str.sale_amount',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'total_quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'cob.total_quantity = str.quantity',
            'target_table': 'cob'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'order_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'coib.order_id = str.transaction_id',
            'target_table': 'coib'
        },
        {
            'source_column': "['str.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'coib.product_id = str.product_id',
            'target_table': 'coib'
        },
        {
            'source_column': "['str.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_accepted',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_accepted',
            'transformation': 'coib.quantity = str.quantity',
            'target_table': 'coib'
        },
        {
            'source_column': "['str.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'line_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'coib.line_amount = str.sale_amount',
            'target_table': 'coib'
        },
        {
            'source_column': "['pr.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.product_id = pr.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.product_name = pr.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.category']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.category = pr.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.brand']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.brand = pr.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.price']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_accepted',
            'target_column': 'price',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.price = pr.price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pr.is_active']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'not_accepted',
            'target_column': 'is_active',
            'target_type': 'BOOLEAN',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.is_active = pr.is_active',
            'target_table': 'pb'
        },
        {
            'source_column': '[]',
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'source_system_table',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': "elaob.source_system_table = 'sales_transactions_raw'",
            'target_table': 'elaob'
        },
        {
            'source_column': "['str.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_accepted',
            'target_column': 'load_watermark_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_accepted',
            'transformation': 'elaob.load_watermark_time = str.transaction_time',
            'target_table': 'elaob'
        },
        {
            'source_column': "['str.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_accepted',
            'target_column': 'reference_key',
            'target_type': 'STRING',
            'target_nullable': 'not_accepted',
            'transformation': 'elaob.reference_key = str.transaction_id',
            'target_table': 'elaob'
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
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            if '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
            else:
                rhs = transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
