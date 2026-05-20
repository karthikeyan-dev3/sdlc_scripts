from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'patient_enrollment_bronze', 'target_alias': 'peb', 'mapping_details': 'patient_enrollment_raw_2000 pe', 'description': 'Bronze ingestion of raw patient enrollment records at patient/trial/site grain. Columns mapped from patient_enrollment_raw_2000: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'}], 'columns': [{'source_column': "['peb.patient_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not_null', 'target_column': 'patient_id', 'target_type': 'varchar(20)', 'target_nullable': 'not_null', 'transformation': 'peb.patient_id = peb.patient_id', 'target_table': 'peb'}, {'source_column': "['peb.trial_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not_null', 'target_column': 'trial_id', 'target_type': 'varchar(20)', 'target_nullable': 'not_null', 'transformation': 'peb.trial_id = peb.trial_id', 'target_table': 'peb'}, {'source_column': "['peb.site_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not_null', 'target_column': 'site_id', 'target_type': 'varchar(20)', 'target_nullable': 'not_null', 'transformation': 'peb.site_id = peb.site_id', 'target_table': 'peb'}, {'source_column': "['peb.patient_name']", 'source_type': 'varchar(255)', 'source_nullable': 'nan', 'target_column': 'patient_name', 'target_type': 'varchar(255)', 'target_nullable': 'nan', 'transformation': 'peb.patient_name = peb.patient_name', 'target_table': 'peb'}, {'source_column': "['peb.gender']", 'source_type': 'varchar(10)', 'source_nullable': 'nan', 'target_column': 'gender', 'target_type': 'varchar(10)', 'target_nullable': 'nan', 'transformation': 'peb.gender = peb.gender', 'target_table': 'peb'}, {'source_column': "['peb.date_of_birth']", 'source_type': 'date', 'source_nullable': 'nan', 'target_column': 'date_of_birth', 'target_type': 'date', 'target_nullable': 'nan', 'transformation': 'peb.date_of_birth = peb.date_of_birth', 'target_table': 'peb'}, {'source_column': "['peb.country']", 'source_type': 'varchar(100)', 'source_nullable': 'nan', 'target_column': 'country', 'target_type': 'varchar(100)', 'target_nullable': 'nan', 'transformation': 'peb.country = peb.country', 'target_table': 'peb'}, {'source_column': "['peb.enrollment_date']", 'source_type': 'timestamp', 'source_nullable': 'nan', 'target_column': 'enrollment_date', 'target_type': 'timestamp', 'target_nullable': 'nan', 'transformation': 'peb.enrollment_date = peb.enrollment_date', 'target_table': 'peb'}, {'source_column': "['peb.consent_status']", 'source_type': 'varchar(20)', 'source_nullable': 'nan', 'target_column': 'consent_status', 'target_type': 'varchar(20)', 'target_nullable': 'nan', 'transformation': 'peb.consent_status = peb.consent_status', 'target_table': 'peb'}, {'source_column': "['peb.source_system']", 'source_type': 'varchar(50)', 'source_nullable': 'nan', 'target_column': 'source_system', 'target_type': 'varchar(50)', 'target_nullable': 'nan', 'transformation': 'peb.source_system = peb.source_system', 'target_table': 'peb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/clinical_trail/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    mapping_details = table.get('mapping_details', '').strip().split()
    source_table = mapping_details[0] if len(mapping_details) > 0 else None
    source_alias = mapping_details[1] if len(mapping_details) > 1 else None

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(f"{base_path}{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()