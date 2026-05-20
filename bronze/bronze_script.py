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
            'target_table': 'patient_enrollment_bronze',
            'target_alias': 'peb',
            'mapping_details': 'patient_enrollment_raw_2000 per',
            'description': 'Bronze table for patient_enrollment entity sourced from patient_enrollment_raw_2000. Columns mapped: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'lab_results_bronze',
            'target_alias': 'lrb',
            'mapping_details': 'lab_results_raw_2000 lrr',
            'description': 'Bronze table for lab_results entity sourced from lab_results_raw_2000. Columns mapped: lab_result_id, patient_id, sample_id, test_name, test_result, test_unit, reference_range, abnormal_flag, test_date, lab_name.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'adverse_events_bronze',
            'target_alias': 'aeb',
            'mapping_details': 'adverse_events_raw_2000 aer',
            'description': 'Bronze table for adverse_events entity sourced from adverse_events_raw_2000. Columns mapped: event_id, patient_id, event_type, severity, event_start_date, event_end_date, outcome, related_to_drug, hospitalization_required, reported_by.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'wearable_device_data_bronze',
            'target_alias': 'wddb',
            'mapping_details': 'wearable_monitoring_raw_2000 wmr',
            'description': 'Bronze table for wearable_device_data entity sourced from wearable_monitoring_raw_2000. Columns mapped: device_record_id, patient_id, device_type, recorded_timestamp, glucose_level, step_count, sleep_hours, heart_rate, battery_status.'
        }
    ],
    'columns': [
        {
            'source_column': "['per.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.patient_id = per.patient_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.trial_id = per.trial_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.site_id = per.site_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'peb.patient_name = per.patient_name',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 'peb.gender = per.gender',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not specified',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not specified',
            'transformation': 'peb.date_of_birth = per.date_of_birth',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'peb.country = per.country',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'peb.enrollment_date = per.enrollment_date',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'peb.consent_status = per.consent_status',
            'target_table': 'peb'
        },
        {
            'source_column': "['per.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not specified',
            'transformation': 'peb.source_system = per.source_system',
            'target_table': 'peb'
        },
        {
            'source_column': "['lrr.lab_result_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'lab_result_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.lab_result_id = lrr.lab_result_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.patient_id = lrr.patient_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.sample_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'sample_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.sample_id = lrr.sample_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.test_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'test_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.test_name = lrr.test_name',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.test_result']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'test_result',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'lrb.test_result = lrr.test_result',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.test_unit']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'test_unit',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.test_unit = lrr.test_unit',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.reference_range']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'reference_range',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.reference_range = lrr.reference_range',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.abnormal_flag']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'abnormal_flag',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.abnormal_flag = lrr.abnormal_flag',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.test_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'test_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'lrb.test_date = lrr.test_date',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrr.lab_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'lab_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'lrb.lab_name = lrr.lab_name',
            'target_table': 'lrb'
        },
        {
            'source_column': "['aer.event_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'event_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_id = aer.event_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.patient_id = aer.patient_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.event_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'event_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_type = aer.event_type',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.severity']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'severity',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.severity = aer.severity',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.event_start_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'event_start_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_start_date = aer.event_start_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.event_end_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'event_end_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'aeb.event_end_date = aer.event_end_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.outcome']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'outcome',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.outcome = aer.outcome',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.related_to_drug']",
            'source_type': 'boolean',
            'source_nullable': 'not specified',
            'target_column': 'related_to_drug',
            'target_type': 'boolean',
            'target_nullable': 'not specified',
            'transformation': 'aeb.related_to_drug = aer.related_to_drug',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.hospitalization_required']",
            'source_type': 'boolean',
            'source_nullable': 'not specified',
            'target_column': 'hospitalization_required',
            'target_type': 'boolean',
            'target_nullable': 'not specified',
            'transformation': 'aeb.hospitalization_required = aer.hospitalization_required',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aer.reported_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'reported_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'aeb.reported_by = aer.reported_by',
            'target_table': 'aeb'
        },
        {
            'source_column': "['wmr.device_record_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'device_record_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wddb.device_record_id = wmr.device_record_id',
            'target_table': 'wddb'
        },
        {
            'source_column': "['wmr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wddb.patient_id = wmr.patient_id',
            'target_table': 'wddb'
        },
        {
            'source_column': "['wmr.device_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'device_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wddb.device_type = wmr.device_type',
            'target_table': 'wddb'
        },
        {
            'source_column': "['wmr.recorded_timestamp']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'recorded_timestamp',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'wddb.recorded_timestamp = wmr.recorded_timestamp',
            'target_table': 'wddb'
        },
        {
            'source_column': "['wmr.glucose_level']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'glucose_level',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wddb.glucose_level = wmr.glucose_level',
            'target_table': 'wddb'
        },
        {
            'source_column': "['wmr.step_count']",
            'source_type': 'int',
            'source_nullable': 'not specified',
            'target_column': 'step_count',
            'target_type': 'int',
            'target_nullable': 'not specified',
            'transformation': 'wddb.step_count = wmr.step_count',
            'target_table': 'wddb'
        },
        {
            'source_column': "['wmr.sleep_hours']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'sleep_hours',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wddb.sleep_hours = wmr.sleep_hours',
            'target_table': 'wddb'
        },
        {
            'source_column': "['wmr.heart_rate']",
            'source_type': 'double',
            'source_nullable': 'not specified',
            'target_column': 'heart_rate',
            'target_type': 'double',
            'target_nullable': 'not specified',
            'transformation': 'wddb.heart_rate = wmr.heart_rate',
            'target_table': 'wddb'
        },
        {
            'source_column': "['wmr.battery_status']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'battery_status',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'wddb.battery_status = wmr.battery_status',
            'target_table': 'wddb'
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

runtime_config = metadata['runtime_config']
base_path = runtime_config['base_path']
target_path = runtime_config['target_path']
read_format = runtime_config['read_format']
write_format = runtime_config['write_format']
write_mode = runtime_config['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + f"{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            transformation = col_meta['transformation']
            rhs = transformation.split('=', 1)[1].strip()
            target_column = col_meta['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + f"{target_table}.{write_format}")

job.commit()
