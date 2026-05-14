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
            "target_table": "adverse_events_bronze",
            "target_alias": "aeb",
            "mapping_details": "adverse_events ae",
            "description": "Bronze ingestion of raw adverse event records from source adverse_events (ae_id, patient_id, drug_id, site_id, symptom, severity, report_date) with no transformations, joins, or aggregations."
        },
        {
            "target_schema": "bronze",
            "target_table": "patient_master_bronze",
            "target_alias": "pmb",
            "mapping_details": "patient_master pm",
            "description": "Bronze ingestion of raw patient master data from source patient_master (patient_id, age, gender, trial_phase, enrollment_date) with no transformations, joins, or aggregations."
        },
        {
            "target_schema": "bronze",
            "target_table": "drug_master_bronze",
            "target_alias": "dmb",
            "mapping_details": "drug_master dm",
            "description": "Bronze ingestion of raw drug reference data from source drug_master (drug_id, drug_name, drug_category, manufacturer) with no transformations, joins, or aggregations."
        },
        {
            "target_schema": "bronze",
            "target_table": "site_master_bronze",
            "target_alias": "smb",
            "mapping_details": "site_master sm",
            "description": "Bronze ingestion of raw site reference data from source site_master (site_id, site_name, city, country) with no transformations, joins, or aggregations."
        }
    ],
    "columns": [
        {
            "source_column": "['ae.ae_id']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "ae_id",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "aeb.ae_id = ae.ae_id",
            "target_table": "aeb"
        },
        {
            "source_column": "['ae.patient_id']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "patient_id",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "aeb.patient_id = ae.patient_id",
            "target_table": "aeb"
        },
        {
            "source_column": "['ae.drug_id']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "drug_id",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "aeb.drug_id = ae.drug_id",
            "target_table": "aeb"
        },
        {
            "source_column": "['ae.site_id']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "site_id",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "aeb.site_id = ae.site_id",
            "target_table": "aeb"
        },
        {
            "source_column": "['ae.symptom']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "symptom",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "aeb.symptom = ae.symptom",
            "target_table": "aeb"
        },
        {
            "source_column": "['ae.severity']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "severity",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "aeb.severity = ae.severity",
            "target_table": "aeb"
        },
        {
            "source_column": "['ae.report_date']",
            "source_type": "date",
            "source_nullable": "not specified",
            "target_column": "report_date",
            "target_type": "date",
            "target_nullable": "not specified",
            "transformation": "aeb.report_date = ae.report_date",
            "target_table": "aeb"
        },
        {
            "source_column": "['pm.patient_id']",
            "source_type": "varchar(10)",
            "source_nullable": "not specified",
            "target_column": "patient_id",
            "target_type": "varchar(10)",
            "target_nullable": "not specified",
            "transformation": "pmb.patient_id = pm.patient_id",
            "target_table": "pmb"
        },
        {
            "source_column": "['pm.age']",
            "source_type": "float",
            "source_nullable": "not specified",
            "target_column": "age",
            "target_type": "float",
            "target_nullable": "not specified",
            "transformation": "pmb.age = pm.age",
            "target_table": "pmb"
        },
        {
            "source_column": "['pm.gender']",
            "source_type": "varchar(10)",
            "source_nullable": "not specified",
            "target_column": "gender",
            "target_type": "varchar(10)",
            "target_nullable": "not specified",
            "transformation": "pmb.gender = pm.gender",
            "target_table": "pmb"
        },
        {
            "source_column": "['pm.trial_phase']",
            "source_type": "varchar(20)",
            "source_nullable": "not specified",
            "target_column": "trial_phase",
            "target_type": "varchar(20)",
            "target_nullable": "not specified",
            "transformation": "pmb.trial_phase = pm.trial_phase",
            "target_table": "pmb"
        },
        {
            "source_column": "['pm.enrollment_date']",
            "source_type": "date",
            "source_nullable": "not specified",
            "target_column": "enrollment_date",
            "target_type": "date",
            "target_nullable": "not specified",
            "transformation": "pmb.enrollment_date = pm.enrollment_date",
            "target_table": "pmb"
        },
        {
            "source_column": "['dm.drug_id']",
            "source_type": "varchar(10)",
            "source_nullable": "not specified",
            "target_column": "drug_id",
            "target_type": "varchar(10)",
            "target_nullable": "not specified",
            "transformation": "dmb.drug_id = dm.drug_id",
            "target_table": "dmb"
        },
        {
            "source_column": "['dm.drug_name']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "drug_name",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "dmb.drug_name = dm.drug_name",
            "target_table": "dmb"
        },
        {
            "source_column": "['dm.drug_category']",
            "source_type": "varchar(100)",
            "source_nullable": "not specified",
            "target_column": "drug_category",
            "target_type": "varchar(100)",
            "target_nullable": "not specified",
            "transformation": "dmb.drug_category = dm.drug_category",
            "target_table": "dmb"
        },
        {
            "source_column": "['dm.manufacturer']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "manufacturer",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "dmb.manufacturer = dm.manufacturer",
            "target_table": "dmb"
        },
        {
            "source_column": "['sm.site_id']",
            "source_type": "varchar(10)",
            "source_nullable": "not specified",
            "target_column": "site_id",
            "target_type": "varchar(10)",
            "target_nullable": "not specified",
            "transformation": "smb.site_id = sm.site_id",
            "target_table": "smb"
        },
        {
            "source_column": "['sm.site_name']",
            "source_type": "varchar(255)",
            "source_nullable": "not specified",
            "target_column": "site_name",
            "target_type": "varchar(255)",
            "target_nullable": "not specified",
            "transformation": "smb.site_name = sm.site_name",
            "target_table": "smb"
        },
        {
            "source_column": "['sm.city']",
            "source_type": "varchar(100)",
            "source_nullable": "not specified",
            "target_column": "city",
            "target_type": "varchar(100)",
            "target_nullable": "not specified",
            "transformation": "smb.city = sm.city",
            "target_table": "smb"
        },
        {
            "source_column": "['sm.country']",
            "source_type": "varchar(100)",
            "source_nullable": "not specified",
            "target_column": "country",
            "target_type": "varchar(100)",
            "target_nullable": "not specified",
            "transformation": "smb.country = sm.country",
            "target_table": "smb"
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
    target_alias = table["target_alias"]

    reader = spark.read.format(read_format)
    if read_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col in metadata["columns"]:
        if col["target_table"] == target_alias:
            rhs = col["transformation"].split("=", 1)[1].strip()
            target_column = col["target_column"]
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == "csv":
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
