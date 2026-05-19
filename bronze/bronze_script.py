```python
from awsglue.context import GlueContext

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
            'target_table': 'patient_enrollment_bronze',
            'target_alias': 'peb',
            'mapping_details': 'patient_enrollment_raw_2000 per',
            'description': 'Bronze ingestion of patient enrollment records from patient_enrollment_raw_2000. Columns mapped: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'lab_results_bronze',
            'target_alias': 'lrb',
            'mapping_details': 'lab_results_raw_2000 lrr',
            'description': 'Bronze ingestion of lab test results from lab_results_raw_2000. Columns mapped: lab_result_id, patient_id, sample_id, test_name, test_result, test_unit, reference_range, abnormal_flag, test_date, lab_name.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'wearable_monitoring_bronze',
            'target_alias': 'wmb',
            'mapping_details': 'wearable_monitoring_raw_2000 wmr',
            'description': 'Bronze ingestion of wearable device monitoring data from wearable_monitoring_raw_2000. Columns mapped: device_record_id, patient_id, device_type, recorded_timestamp, glucose_level, step_count, sleep_hours, heart_rate, battery_status.'
        }
    ],
    'columns': [
        {
            'source_column': "['per.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_provided',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_provided',
            'transformation': 'peb.patient_id = per.patient_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_provided',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_provided',
            'transformation': 'peb.trial_id = per.trial_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_provided',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_provided',
            'transformation': 'peb.site_id = per.site_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'peb.patient_name = per.patient_name',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_provided',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_provided',
            'transformation': 'peb.gender = per.gender',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not_provided',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not_provided',
            'transformation': 'peb.date_of_birth = per.date_of_birth',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_provided',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_provided',
            'transformation': 'peb.country = per.country',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not_provided',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not_provided',
            'transformation': 'peb.enrollment_date = per.enrollment_date',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_provided',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_provided',
            'transformation': 'peb.consent_status = per.consent_status',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_provided',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_provided',
            'transformation': 'peb.source_system = per.source_system',
            'target_table': 'peb'
        },
        {
            'source_column': "['lrr.lab_result_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'lab_result_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.lab_result_id = lrr.lab_result_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.patient_id = lrr.patient_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.sample_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'sample_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.sample_id = lrr.sample_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.test_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'test_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.test_name = lrr.test_name',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.test_result']",
            'source_type': 'double',
            'source_nullable': 'not_provided',
            'target_column': 'test_result',
            'target_type': 'double',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.test_result = lrr.test_result',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.test_unit']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'test_unit',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.test_unit = lrr.test_unit',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.reference_range']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'reference_range',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.reference_range = lrr.reference_range',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.abnormal_flag']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'abnormal_flag',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.abnormal_flag = lrr.abnormal_flag',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.test_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not_provided',
            'target_column': 'test_date',
            'target_type': 'timestamp',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.test_date = lrr.test_date',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.lab_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'lab_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'lrb.lab_name = lrr.lab_name',
            'target_table': 'lrb'
        },
        {
            'source_column': "['wmr.device_record_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'device_record_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.device_record_id = wmr.device_record_id',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wmr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.patient_id = wmr.patient_id',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wmr.device_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'device_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.device_type = wmr.device_type',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wmr.recorded_timestamp']",
            'source_type': 'timestamp',
            'source_nullable': 'not_provided',
            'target_column': 'recorded_timestamp',
            'target_type': 'timestamp',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.recorded_timestamp = wmr.recorded_timestamp',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wmr.glucose_level']",
            'source_type': 'double',
            'source_nullable': 'not_provided',
            'target_column': 'glucose_level',
            'target_type': 'double',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.glucose_level = wmr.glucose_level',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wmr.step_count']",
            'source_type': 'int',
            'source_nullable': 'not_provided',
            'target_column': 'step_count',
            'target_type': 'int',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.step_count = wmr.step_count',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wmr.sleep_hours']",
            'source_type': 'double',
            'source_nullable': 'not_provided',
            'target_column': 'sleep_hours',
            'target_type': 'double',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.sleep_hours = wmr.sleep_hours',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wmr.heart_rate']",
            'source_type': 'double',
            'source_nullable': 'not_provided',
            'target_column': 'heart_rate',
            'target_type': 'double',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.heart_rate = wmr.heart_rate',
            'target_table': 'wmb'
        },
        {
            'source_column': "['wmr.battery_status']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_provided',
            'target_column': 'battery_status',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_provided',
            'transformation': 'wmb.battery_status = wmr.battery_status',
            'target_table': 'wmb'
        }
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/clinical_trail/',
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
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + '.' + read_format)
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
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + '.' + write_format)

job.commit()

```