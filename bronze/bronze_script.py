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
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores_raw s',
            'description': 'Bronze ingestion of stores from stores_raw with columns: store_id, store_name, city, state, store_type, open_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products_raw p',
            'description': 'Bronze ingestion of products from products_raw with columns: product_id, product_name, category, brand, price, is_active.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sales_transactions_bronze',
            'target_alias': 'stb',
            'mapping_details': 'sales_transactions_raw t',
            'description': 'Bronze ingestion of sales transactions from sales_transactions_raw with columns: transaction_id, store_id, product_id, quantity, sale_amount, transaction_time.'
        }
    ],
    'columns': [
        {
            'source_column': "['s.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'sb.store_id = s.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['s.store_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'store_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'sb.store_name = s.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['p.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'pb.product_id = p.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['p.product_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'product_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'pb.product_name = p.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['t.transaction_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'stb.transaction_id = t.transaction_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['t.store_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'store_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'stb.store_id = t.store_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['t.product_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'product_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'stb.product_id = t.product_id',
            'target_table': 'stb'
        },
        {
            'source_column': "['t.transaction_time']",
            'source_type': 'timestamp',
            'source_nullable': 'accepted',
            'target_column': 'transaction_time',
            'target_type': 'timestamp',
            'target_nullable': 'accepted',
            'transformation': 'stb.transaction_time = t.transaction_time',
            'target_table': 'stb'
        },
        {
            'source_column': "['t.quantity']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'quantity',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'stb.quantity = t.quantity',
            'target_table': 'stb'
        },
        {
            'source_column': "['t.sale_amount']",
            'source_type': 'double',
            'source_nullable': 'accepted',
            'target_column': 'sale_amount',
            'target_type': 'double',
            'target_nullable': 'accepted',
            'transformation': 'stb.sale_amount = t.sale_amount',
            'target_table': 'stb'
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

base_path = metadata["runtime_config"]["base_path"]
target_path = metadata["runtime_config"]["target_path"]
read_format = metadata["runtime_config"]["read_format"]
write_format = metadata["runtime_config"]["write_format"]
write_mode = metadata["runtime_config"]["write_mode"]

for table in metadata["tables"]:
    mapping_details = table["mapping_details"].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table["target_table"]
    target_alias = table["target_alias"]

    reader = spark.read.format(read_format)
    if read_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata["columns"]:
        if col_meta["target_table"] == target_alias:
            rhs = col_meta["transformation"].split("=", 1)[1].strip()
            target_column = col_meta["target_column"]
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == "csv":
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()