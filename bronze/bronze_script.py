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
            'target_table': 'customer_orders',
            'target_alias': 'co',
            'mapping_details': 'sales_transactions_raw st',
            'description': 'Bronze ingestion of sales transactions at transaction grain. Map st.transaction_id, st.store_id, st.product_id, st.quantity, st.sale_amount, st.transaction_time directly from sales_transactions_raw with no joins or aggregations.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_orders_dq_summary',
            'target_alias': 'codq',
            'mapping_details': 'sales_transactions_raw st',
            'description': 'Bronze data quality summary placeholder sourced directly from sales_transactions_raw without aggregations or joins. Land raw columns (transaction_id, store_id, product_id, quantity, sale_amount, transaction_time) to support downstream DQ computations in higher layers.'
        }
    ],
    'columns': [
        {
            'source_column': "['st.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'co.transaction_id = st.transaction_id',
            'target_table': 'co'
        },
        {
            'source_column': "['st.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'co.store_id = st.store_id',
            'target_table': 'co'
        },
        {
            'source_column': "['st.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'co.product_id = st.product_id',
            'target_table': 'co'
        },
        {
            'source_column': "['st.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_specified',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_specified',
            'transformation': 'co.quantity = st.quantity',
            'target_table': 'co'
        },
        {
            'source_column': "['st.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_specified',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_specified',
            'transformation': 'co.sale_amount = st.sale_amount',
            'target_table': 'co'
        },
        {
            'source_column': "['st.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_specified',
            'transformation': 'co.transaction_time = st.transaction_time',
            'target_table': 'co'
        },
        {
            'source_column': "['st.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'codq.transaction_id = st.transaction_id',
            'target_table': 'codq'
        },
        {
            'source_column': "['st.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'codq.store_id = st.store_id',
            'target_table': 'codq'
        },
        {
            'source_column': "['st.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_specified',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_specified',
            'transformation': 'codq.product_id = st.product_id',
            'target_table': 'codq'
        },
        {
            'source_column': "['st.quantity']",
            'source_type': 'INT',
            'source_nullable': 'not_specified',
            'target_column': 'quantity',
            'target_type': 'INT',
            'target_nullable': 'not_specified',
            'transformation': 'codq.quantity = st.quantity',
            'target_table': 'codq'
        },
        {
            'source_column': "['st.sale_amount']",
            'source_type': 'DECIMAL',
            'source_nullable': 'not_specified',
            'target_column': 'sale_amount',
            'target_type': 'DECIMAL',
            'target_nullable': 'not_specified',
            'transformation': 'codq.sale_amount = st.sale_amount',
            'target_table': 'codq'
        },
        {
            'source_column': "['st.transaction_time']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_time',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_specified',
            'transformation': 'codq.transaction_time = st.transaction_time',
            'target_table': 'codq'
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
    parts = mapping_details.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

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
            if '=' in transformation:
                rhs = transformation.split('=', 1)[1].strip()
            else:
                rhs = transformation.strip()
            target_column = col.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
