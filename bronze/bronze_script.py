from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'patient_enrollment_bronze', 'target_alias': 'peb', 'mapping_details': 'patient_enrollment_raw_2000 pe', 'description': 'Bronze ingestion of patient enrollment records from patient_enrollment_raw_2000 with columns: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'}, {'target_schema': 'bronze', 'target_table': 'clinical_visit_bronze', 'target_alias': 'cvb', 'mapping_details': 'clinical_visit_raw_2000 cv', 'description': 'Bronze ingestion of clinical visit records from clinical_visit_raw_2000 with columns: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'}], 'columns': [{'source_column': "['pe.patient_id']", 'source_type': 'varchar(20)', 'source_nullable': 'null_accepted', 'target_column': 'patient_id', 'target_type': 'varchar(20)', 'target_nullable': 'null_accepted', 'transformation': 'peb.patient_id = pe.patient_id', 'target_table': 'peb'}, {'source_column': "['pe.trial_id']", 'source_type': 'varchar(20)', 'source_nullable': 'null_accepted', 'target_column': 'trial_id', 'target_type': 'varchar(20)', 'target_nullable': 'null_accepted', 'transformation': 'peb.trial_id = pe.trial_id', 'target_table': 'peb'}, {'source_column': "['pe.site_id']", 'source_type': 'varchar(20)', 'source_nullable': 'null_accepted', 'target_column': 'site_id', 'target_type': 'varchar(20)', 'target_nullable': 'null_accepted', 'transformation': 'peb.site_id = pe.site_id', 'target_table': 'peb'}, {'source_column': "['pe.patient_name']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'patient_name', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'peb.patient_name = pe.patient_name', 'target_table': 'peb'}, {'source_column': "['pe.gender']", 'source_type': 'varchar(10)', 'source_nullable': 'null_accepted', 'target_column': 'gender', 'target_type': 'varchar(10)', 'target_nullable': 'null_accepted', 'transformation': 'peb.gender = pe.gender', 'target_table': 'peb'}, {'source_column': "['pe.date_of_birth']", 'source_type': 'date', 'source_nullable': 'null_accepted', 'target_column': 'date_of_birth', 'target_type': 'date', 'target_nullable': 'null_accepted', 'transformation': 'peb.date_of_birth = pe.date_of_birth', 'target_table': 'peb'}, {'source_column': "['pe.country']", 'source_type': 'varchar(100)', 'source_nullable': 'null_accepted', 'target_column': 'country', 'target_type': 'varchar(100)', 'target_nullable': 'null_accepted', 'transformation': 'peb.country = pe.country', 'target_table': 'peb'}, {'source_column': "['pe.enrollment_date']", 'source_type': 'timestamp', 'source_nullable': 'null_accepted', 'target_column': 'enrollment_date', 'target_type': 'timestamp', 'target_nullable': 'null_accepted', 'transformation': 'peb.enrollment_date = pe.enrollment_date', 'target_table': 'peb'}, {'source_column': "['pe.consent_status']", 'source_type': 'varchar(20)', 'source_nullable': 'null_accepted', 'target_column': 'consent_status', 'target_type': 'varchar(20)', 'target_nullable': 'null_accepted', 'transformation': 'peb.consent_status = pe.consent_status', 'target_table': 'peb'}, {'source_column': "['pe.source_system']", 'source_type': 'varchar(50)', 'source_nullable': 'null_accepted', 'target_column': 'source_system', 'target_type': 'varchar(50)', 'target_nullable': 'null_accepted', 'transformation': 'peb.source_system = pe.source_system', 'target_table': 'peb'}, {'source_column': "['cv.visit_id']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'visit_id', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'cvb.visit_id = cv.visit_id', 'target_table': 'cvb'}, {'source_column': "['cv.patient_id']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'patient_id', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'cvb.patient_id = cv.patient_id', 'target_table': 'cvb'}, {'source_column': "['cv.trial_id']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'trial_id', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'cvb.trial_id = cv.trial_id', 'target_table': 'cvb'}, {'source_column': "['cv.visit_date']", 'source_type': 'timestamp', 'source_nullable': 'null_accepted', 'target_column': 'visit_date', 'target_type': 'timestamp', 'target_nullable': 'null_accepted', 'transformation': 'cvb.visit_date = cv.visit_date', 'target_table': 'cvb'}, {'source_column': "['cv.visit_type']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'visit_type', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'cvb.visit_type = cv.visit_type', 'target_table': 'cvb'}, {'source_column': "['cv.blood_pressure']", 'source_type': 'float64', 'source_nullable': 'null_accepted', 'target_column': 'blood_pressure', 'target_type': 'float64', 'target_nullable': 'null_accepted', 'transformation': 'cvb.blood_pressure = cv.blood_pressure', 'target_table': 'cvb'}, {'source_column': "['cv.heart_rate']", 'source_type': 'float64', 'source_nullable': 'null_accepted', 'target_column': 'heart_rate', 'target_type': 'float64', 'target_nullable': 'null_accepted', 'transformation': 'cvb.heart_rate = cv.heart_rate', 'target_table': 'cvb'}, {'source_column': "['cv.weight_kg']", 'source_type': 'float64', 'source_nullable': 'null_accepted', 'target_column': 'weight_kg', 'target_type': 'float64', 'target_nullable': 'null_accepted', 'transformation': 'cvb.weight_kg = cv.weight_kg', 'target_table': 'cvb'}, {'source_column': "['cv.physician_notes']", 'source_type': 'text', 'source_nullable': 'null_accepted', 'target_column': 'physician_notes', 'target_type': 'text', 'target_nullable': 'null_accepted', 'transformation': 'cvb.physician_notes = cv.physician_notes', 'target_table': 'cvb'}, {'source_column': "['cv.source_system']", 'source_type': 'varchar(255)', 'source_nullable': 'null_accepted', 'target_column': 'source_system', 'target_type': 'varchar(255)', 'target_nullable': 'null_accepted', 'transformation': 'cvb.source_system = cv.source_system', 'target_table': 'cvb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/clinical_trail/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details']
    source_table, source_alias = mapping_details.split()
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")
    df = reader.load(f"{base_path}{source_table}.{read_format}")

    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            transformation = col_meta['transformation']
            target_column = col_meta['target_column']
            rhs = transformation.split('=', 1)[1].strip()
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")
    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()
