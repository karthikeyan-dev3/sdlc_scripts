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
            "target_table": "product_bronze",
            "target_alias": "pb",
            "mapping_details": "products_raw pr",
            "description": "Bronze ingestion of raw product records at row-level grain. Direct mapping from products_raw without joins or aggregations."
        },
        {
            "target_schema": "bronze",
            "target_table": "store_bronze",
            "target_alias": "sb",
            "mapping_details": "stores_raw sr",
            "description": "Bronze ingestion of raw store records at row-level grain. Direct mapping from stores_raw without joins or aggregations."
        },
        {
            "target_schema": "bronze",
            "target_table": "sales_bronze",
            "target_alias": "slb",
            "mapping_details": "sales_transactions_raw str",
            "description": "Bronze ingestion of raw sales transaction records at row-level grain. Direct mapping from sales_transactions_raw without joins or aggregations."
        }
    ],
    "columns": [
        {
            "source_column": "['pr.product_id']",
            "source_type": "varchar(10)",
            "source_nullable": "not_specified",
            "target_column": "product_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_specified",
            "transformation": "pr.product_id = pb.product_id",
            "target_table": "pr"
        },
        {
            "source_column": "['pr.product_name']",
            "source_type": "varchar(255)",
            "source_nullable": "not_specified",
            "target_column": "product_name",
            "target_type": "varchar(255)",
            "target_nullable": "not_specified",
            "transformation": "pr.product_name = pb.product_name",
            "target_table": "pr"
        },
        {
            "source_column": "['pr.category']",
            "source_type": "varchar(100)",
            "source_nullable": "not_specified",
            "target_column": "category",
            "target_type": "varchar(100)",
            "target_nullable": "not_specified",
            "transformation": "pr.category = pb.category",
            "target_table": "pr"
        },
        {
            "source_column": "['pr.brand']",
            "source_type": "varchar(100)",
            "source_nullable": "not_specified",
            "target_column": "brand",
            "target_type": "varchar(100)",
            "target_nullable": "not_specified",
            "transformation": "pr.brand = pb.brand",
            "target_table": "pr"
        },
        {
            "source_column": "['pr.price']",
            "source_type": "float",
            "source_nullable": "not_specified",
            "target_column": "price",
            "target_type": "float",
            "target_nullable": "not_specified",
            "transformation": "pr.price = pb.price",
            "target_table": "pr"
        },
        {
            "source_column": "['pr.is_active']",
            "source_type": "boolean",
            "source_nullable": "not_specified",
            "target_column": "is_active",
            "target_type": "boolean",
            "target_nullable": "not_specified",
            "transformation": "pr.is_active = pb.is_active",
            "target_table": "pr"
        },
        {
            "source_column": "['sr.store_id']",
            "source_type": "varchar(10)",
            "source_nullable": "not_specified",
            "target_column": "store_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_specified",
            "transformation": "sr.store_id = sb.store_id",
            "target_table": "sr"
        },
        {
            "source_column": "['sr.store_name']",
            "source_type": "varchar(255)",
            "source_nullable": "not_specified",
            "target_column": "store_name",
            "target_type": "varchar(255)",
            "target_nullable": "not_specified",
            "transformation": "sr.store_name = sb.store_name",
            "target_table": "sr"
        },
        {
            "source_column": "['sr.city']",
            "source_type": "varchar(100)",
            "source_nullable": "not_specified",
            "target_column": "city",
            "target_type": "varchar(100)",
            "target_nullable": "not_specified",
            "transformation": "sr.city = sb.city",
            "target_table": "sr"
        },
        {
            "source_column": "['sr.state']",
            "source_type": "varchar(100)",
            "source_nullable": "not_specified",
            "target_column": "state",
            "target_type": "varchar(100)",
            "target_nullable": "not_specified",
            "transformation": "sr.state = sb.state",
            "target_table": "sr"
        },
        {
            "source_column": "['sr.store_type']",
            "source_type": "varchar(50)",
            "source_nullable": "not_specified",
            "target_column": "store_type",
            "target_type": "varchar(50)",
            "target_nullable": "not_specified",
            "transformation": "sr.store_type = sb.store_type",
            "target_table": "sr"
        },
        {
            "source_column": "['sr.open_date']",
            "source_type": "date",
            "source_nullable": "not_specified",
            "target_column": "open_date",
            "target_type": "date",
            "target_nullable": "not_specified",
            "transformation": "sr.open_date = sb.open_date",
            "target_table": "sr"
        },
        {
            "source_column": "['str.transaction_id']",
            "source_type": "varchar(10)",
            "source_nullable": "not_specified",
            "target_column": "transaction_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_specified",
            "transformation": "str.transaction_id = slb.transaction_id",
            "target_table": "str"
        },
        {
            "source_column": "['str.store_id']",
            "source_type": "varchar(10)",
            "source_nullable": "not_specified",
            "target_column": "store_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_specified",
            "transformation": "str.store_id = slb.store_id",
            "target_table": "str"
        },
        {
            "source_column": "['str.product_id']",
            "source_type": "varchar(10)",
            "source_nullable": "not_specified",
            "target_column": "product_id",
            "target_type": "varchar(10)",
            "target_nullable": "not_specified",
            "transformation": "str.product_id = slb.product_id",
            "target_table": "str"
        },
        {
            "source_column": "['str.quantity']",
            "source_type": "int",
            "source_nullable": "not_specified",
            "target_column": "quantity",
            "target_type": "int",
            "target_nullable": "not_specified",
            "transformation": "str.quantity = slb.quantity",
            "target_table": "str"
        },
        {
            "source_column": "['str.sale_amount']",
            "source_type": "double",
            "source_nullable": "not_specified",
            "target_column": "sale_amount",
            "target_type": "double",
            "target_nullable": "not_specified",
            "transformation": "str.sale_amount = slb.sale_amount",
            "target_table": "str"
        },
        {
            "source_column": "['str.transaction_time']",
            "source_type": "timestamp",
            "source_nullable": "not_specified",
            "target_column": "transaction_time",
            "target_type": "timestamp",
            "target_nullable": "not_specified",
            "transformation": "str.transaction_time = slb.transaction_time",
            "target_table": "str"
        }
    ],
    "runtime_config": {
        "base_path": "s3://sdlc-agent-bucket/engineering-agent/src/",
        "target_path": "s3://sdlc-agent-bucket/engineering-agent/bronze/",
        "read_format": "csv",
        "write_format": "csv",
        "write_mode": "overwrite"
    }
}

runtime_config = metadata["runtime_config"]
base_path = runtime_config["base_path"]
target_path = runtime_config["target_path"]
read_format = runtime_config["read_format"]
write_format = runtime_config["write_format"]
write_mode = runtime_config["write_mode"]

for table in metadata["tables"]:
    mapping_details = table["mapping_details"].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table["target_table"]

    reader = spark.read.format(read_format)
    if read_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata["columns"]:
        if col_meta.get("target_table") == source_alias:
            transformation = col_meta["transformation"]
            rhs = transformation.split("=", 1)[1].strip()
            target_column = col_meta["target_column"]
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == "csv":
        writer = writer.option("header", "true")

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
