from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {
    "tables": [
        {
            "target_schema": "bronze",
            "target_table": "stores_bronze",
            "target_alias": "stb",
            "mapping_details": "stores_raw sr",
            "description": "Bronze table for stores entity. 1:1 raw ingestion from stores_raw with all source columns preserved.",
        },
        {
            "target_schema": "bronze",
            "target_table": "products_bronze",
            "target_alias": "prb",
            "mapping_details": "products_raw pr",
            "description": "Bronze table for products entity. 1:1 raw ingestion from products_raw with all source columns preserved.",
        },
        {
            "target_schema": "bronze",
            "target_table": "sales_transactions_bronze",
            "target_alias": "stxb",
            "mapping_details": "sales_transactions_raw str",
            "description": "Bronze table for sales_transactions entity. 1:1 raw ingestion from sales_transactions_raw with all source columns preserved.",
        },
    ],
    "columns": [
        {
            "source_column": "['sr.store_id']",
            "source_type": "STRING",
            "source_nullable": "not_null",
            "target_column": "store_id",
            "target_type": "STRING",
            "target_nullable": "not_null",
            "transformation": "stb.store_id = sr.store_id",
            "target_table": "stb",
        },
        {
            "source_column": "['sr.store_name']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "store_name",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "stb.store_name = sr.store_name",
            "target_table": "stb",
        },
        {
            "source_column": "['sr.city']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "city",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "stb.city = sr.city",
            "target_table": "stb",
        },
        {
            "source_column": "['sr.state']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "state",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "stb.state = sr.state",
            "target_table": "stb",
        },
        {
            "source_column": "['sr.store_type']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "store_type",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "stb.store_type = sr.store_type",
            "target_table": "stb",
        },
        {
            "source_column": "['sr.open_date']",
            "source_type": "DATE",
            "source_nullable": "null_accepted",
            "target_column": "open_date",
            "target_type": "DATE",
            "target_nullable": "null_accepted",
            "transformation": "stb.open_date = sr.open_date",
            "target_table": "stb",
        },
        {
            "source_column": "['pr.product_id']",
            "source_type": "STRING",
            "source_nullable": "not_null",
            "target_column": "product_id",
            "target_type": "STRING",
            "target_nullable": "not_null",
            "transformation": "prb.product_id = pr.product_id",
            "target_table": "prb",
        },
        {
            "source_column": "['pr.product_name']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "product_name",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "prb.product_name = pr.product_name",
            "target_table": "prb",
        },
        {
            "source_column": "['pr.category']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "category",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "prb.category = pr.category",
            "target_table": "prb",
        },
        {
            "source_column": "['pr.brand']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "brand",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "prb.brand = pr.brand",
            "target_table": "prb",
        },
        {
            "source_column": "['pr.price']",
            "source_type": "DECIMAL",
            "source_nullable": "null_accepted",
            "target_column": "price",
            "target_type": "DECIMAL",
            "target_nullable": "null_accepted",
            "transformation": "prb.price = pr.price",
            "target_table": "prb",
        },
        {
            "source_column": "['pr.is_active']",
            "source_type": "BOOLEAN",
            "source_nullable": "null_accepted",
            "target_column": "is_active",
            "target_type": "BOOLEAN",
            "target_nullable": "null_accepted",
            "transformation": "prb.is_active = pr.is_active",
            "target_table": "prb",
        },
        {
            "source_column": "['str.transaction_id']",
            "source_type": "STRING",
            "source_nullable": "not_null",
            "target_column": "transaction_id",
            "target_type": "STRING",
            "target_nullable": "not_null",
            "transformation": "stxb.transaction_id = str.transaction_id",
            "target_table": "stxb",
        },
        {
            "source_column": "['str.store_id']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "store_id",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "stxb.store_id = str.store_id",
            "target_table": "stxb",
        },
        {
            "source_column": "['str.product_id']",
            "source_type": "STRING",
            "source_nullable": "null_accepted",
            "target_column": "product_id",
            "target_type": "STRING",
            "target_nullable": "null_accepted",
            "transformation": "stxb.product_id = str.product_id",
            "target_table": "stxb",
        },
        {
            "source_column": "['str.quantity']",
            "source_type": "INT",
            "source_nullable": "null_accepted",
            "target_column": "quantity",
            "target_type": "INT",
            "target_nullable": "null_accepted",
            "transformation": "stxb.quantity = str.quantity",
            "target_table": "stxb",
        },
        {
            "source_column": "['str.sale_amount']",
            "source_type": "DECIMAL",
            "source_nullable": "null_accepted",
            "target_column": "sale_amount",
            "target_type": "DECIMAL",
            "target_nullable": "null_accepted",
            "transformation": "stxb.sale_amount = str.sale_amount",
            "target_table": "stxb",
        },
        {
            "source_column": "['str.transaction_time']",
            "source_type": "TIMESTAMP",
            "source_nullable": "null_accepted",
            "target_column": "transaction_time",
            "target_type": "TIMESTAMP",
            "target_nullable": "null_accepted",
            "transformation": "stxb.transaction_time = str.transaction_time",
            "target_table": "stxb",
        },
    ],
    "runtime_config": {
        "base_path": "s3://sdlc-agent-bucket/engineering-agent/src/",
        "target_path": "s3://sdlc-agent-bucket/engineering-agent/bronze/",
        "read_format": "csv",
        "write_format": "csv",
        "write_mode": "overwrite",
    },
}

runtime_config = metadata.get("runtime_config", {})
base_path = runtime_config.get("base_path")
target_path = runtime_config.get("target_path")
read_format = runtime_config.get("read_format")
write_format = runtime_config.get("write_format")
write_mode = runtime_config.get("write_mode")

for table in metadata.get("tables", []):
    mapping_details = table.get("mapping_details", "")
    mapping_parts = mapping_details.split()
    source_table = mapping_parts[0] if len(mapping_parts) > 0 else None
    source_alias = mapping_parts[1] if len(mapping_parts) > 1 else None

    target_table = table.get("target_table")
    target_alias = table.get("target_alias")

    reader = spark.read.format(read_format)
    if read_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get("columns", []):
        if col_meta.get("target_table") == target_alias:
            transformation = col_meta.get("transformation", "")
            rhs = transformation.split("=", 1)[1].strip() if "=" in transformation else transformation.strip()
            target_column = col_meta.get("target_column")
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == "csv":
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()