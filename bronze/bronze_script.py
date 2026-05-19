from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("bronze_job", {})

metadata = {'tables': [{'target_schema': 'bronze', 'target_table': 'patients_bronze', 'target_alias': 'pb', 'mapping_details': 'patient_enrollment_raw_2000 per', 'description': 'Bronze patients table sourced from patient_enrollment_raw_2000 per. Columns mapped: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'}, {'target_schema': 'bronze', 'target_table': 'health_metrics_bronze', 'target_alias': 'hmb', 'mapping_details': 'wearable_monitoring_raw_2000 wmr', 'description': 'Bronze health metrics table sourced from wearable_monitoring_raw_2000 wmr. Columns mapped: device_record_id, patient_id, device_type, recorded_timestamp, glucose_level, step_count, sleep_hours, heart_rate, battery_status.'}], 'columns': [{'source_column': "['per.patient_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not specified', 'target_column': 'patient_id', 'target_type': 'varchar(20)', 'target_nullable': 'not specified', 'transformation': 'pb.patient_id = per.patient_id', 'target_table': 'pb'}, {'source_column': "['per.trial_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not specified', 'target_column': 'trial_id', 'target_type': 'varchar(20)', 'target_nullable': 'not specified', 'transformation': 'pb.trial_id = per.trial_id', 'target_table': 'pb'}, {'source_column': "['per.site_id']", 'source_type': 'varchar(20)', 'source_nullable': 'not specified', 'target_column': 'site_id', 'target_type': 'varchar(20)', 'target_nullable': 'not specified', 'transformation': 'pb.site_id = per.site_id', 'target_table': 'pb'}, {'source_column': "['per.patient_name']", 'source_type': 'varchar(255)', 'source_nullable': 'not specified', 'target_column': 'patient_name', 'target_type': 'varchar(255)', 'target_nullable': 'not specified', 'transformation': 'pb.patient_name = per.patient_name', 'target_table': 'pb'}, {'source_column': "['per.gender']", 'source_type': 'varchar(10)', 'source_nullable': 'not specified', 'target_column': 'gender', 'target_type': 'varchar(10)', 'target_nullable': 'not specified', 'transformation': 'pb.gender = per.gender', 'target_table': 'pb'}, {'source_column': "['per.date_of_birth']", 'source_type': 'date', 'source_nullable': 'not specified', 'target_column': 'date_of_birth', 'target_type': 'date', 'target_nullable': 'not specified', 'transformation': 'pb.date_of_birth = per.date_of_birth', 'target_table': 'pb'}, {'source_column': "['per.country']", 'source_type': 'varchar(100)', 'source_nullable': 'not specified', 'target_column': 'country', 'target_type': 'varchar(100)', 'target_nullable': 'not specified', 'transformation': 'pb.country = per.country', 'target_table': 'pb'}, {'source_column': "['per.enrollment_date']", 'source_type': 'timestamp', 'source_nullable': 'not specified', 'target_column': 'enrollment_date', 'target_type': 'timestamp', 'target_nullable': 'not specified', 'transformation': 'pb.enrollment_date = per.enrollment_date', 'target_table': 'pb'}, {'source_column': "['per.consent_status']", 'source_type': 'varchar(20)', 'source_nullable': 'not specified', 'target_column': 'consent_status', 'target_type': 'varchar(20)', 'target_nullable': 'not specified', 'transformation': 'pb.consent_status = per.consent_status', 'target_table': 'pb'}, {'source_column': "['per.source_system']", 'source_type': 'varchar(50)', 'source_nullable': 'not specified', 'target_column': 'source_system', 'target_type': 'varchar(50)', 'target_nullable': 'not specified', 'transformation': 'pb.source_system = per.source_system', 'target_table': 'pb'}, {'source_column': "['wmr.device_record_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not specified', 'target_column': 'device_record_id', 'target_type': 'varchar(255)', 'target_nullable': 'not specified', 'transformation': 'hmb.device_record_id = wmr.device_record_id', 'target_table': 'hmb'}, {'source_column': "['wmr.patient_id']", 'source_type': 'varchar(255)', 'source_nullable': 'not specified', 'target_column': 'patient_id', 'target_type': 'varchar(255)', 'target_nullable': 'not specified', 'transformation': 'hmb.patient_id = wmr.patient_id', 'target_table': 'hmb'}, {'source_column': "['wmr.device_type']", 'source_type': 'varchar(255)', 'source_nullable': 'not specified', 'target_column': 'device_type', 'target_type': 'varchar(255)', 'target_nullable': 'not specified', 'transformation': 'hmb.device_type = wmr.device_type', 'target_table': 'hmb'}, {'source_column': "['wmr.recorded_timestamp']", 'source_type': 'timestamp', 'source_nullable': 'not specified', 'target_column': 'recorded_timestamp', 'target_type': 'timestamp', 'target_nullable': 'not specified', 'transformation': 'hmb.recorded_timestamp = wmr.recorded_timestamp', 'target_table': 'hmb'}, {'source_column': "['wmr.glucose_level']", 'source_type': 'double', 'source_nullable': 'not specified', 'target_column': 'glucose_level', 'target_type': 'double', 'target_nullable': 'not specified', 'transformation': 'hmb.glucose_level = wmr.glucose_level', 'target_table': 'hmb'}, {'source_column': "['wmr.step_count']", 'source_type': 'int', 'source_nullable': 'not specified', 'target_column': 'step_count', 'target_type': 'int', 'target_nullable': 'not specified', 'transformation': 'hmb.step_count = wmr.step_count', 'target_table': 'hmb'}, {'source_column': "['wmr.sleep_hours']", 'source_type': 'double', 'source_nullable': 'not specified', 'target_column': 'sleep_hours', 'target_type': 'double', 'target_nullable': 'not specified', 'transformation': 'hmb.sleep_hours = wmr.sleep_hours', 'target_table': 'hmb'}, {'source_column': "['wmr.heart_rate']", 'source_type': 'double', 'source_nullable': 'not specified', 'target_column': 'heart_rate', 'target_type': 'double', 'target_nullable': 'not specified', 'transformation': 'hmb.heart_rate = wmr.heart_rate', 'target_table': 'hmb'}, {'source_column': "['wmr.battery_status']", 'source_type': 'varchar(255)', 'source_nullable': 'not specified', 'target_column': 'battery_status', 'target_type': 'varchar(255)', 'target_nullable': 'not specified', 'transformation': 'hmb.battery_status = wmr.battery_status', 'target_table': 'hmb'}], 'runtime_config': {'base_path': 's3://sdlc-agent-bucket/engineering-agent/clinical_trail/', 'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/', 'read_format': 'csv', 'write_format': 'csv', 'write_mode': 'overwrite'}}

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
    for col in metadata.get('columns', []):
        if col.get('target_table') == target_alias:
            transformation = col.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation.strip()
            target_column = col.get('target_column')
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()
