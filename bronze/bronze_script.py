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
            'target_table': 'clinical_trials_bronze',
            'target_alias': 'ctb',
            'mapping_details': 'patient_enrollment_raw_2000 per',
            'description': 'Bronze table for Clinical Trials entity sourced from patient_enrollment_raw_2000. Columns: trial_id, site_id, patient_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'sites_bronze',
            'target_alias': 'sb',
            'mapping_details': 'patient_enrollment_raw_2000 per',
            'description': 'Bronze table for Sites entity sourced from patient_enrollment_raw_2000. Columns: site_id, trial_id, patient_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'countries_bronze',
            'target_alias': 'cob',
            'mapping_details': 'patient_enrollment_raw_2000 per',
            'description': 'Bronze table for Countries entity sourced from patient_enrollment_raw_2000. Columns: country, trial_id, site_id, patient_id, patient_name, gender, date_of_birth, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'metrics_bronze',
            'target_alias': 'mb',
            'mapping_details': 'wearable_monitoring_raw_2000 wmr',
            'description': 'Bronze table for Metrics entity sourced from wearable_monitoring_raw_2000. Columns: device_record_id, patient_id, device_type, recorded_timestamp, glucose_level, step_count, sleep_hours, heart_rate, battery_status.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'data_management_bronze',
            'target_alias': 'dmb',
            'mapping_details': 'clinical_visit_raw_2000 cvr',
            'description': 'Bronze table for Data Management entity sourced from clinical_visit_raw_2000 to support freshness/quality monitoring at ingestion. Columns: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'
        }
    ],
    'columns': [
        {
            'source_column': "['per.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'ctb.trial_id = per.trial_id',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'ctb.site_id = per.site_id',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'ctb.patient_id = per.patient_id',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'ctb.patient_name = per.patient_name',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 'ctb.gender = per.gender',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not specified',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not specified',
            'transformation': 'ctb.date_of_birth = per.date_of_birth',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'ctb.country = per.country',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'ctb.enrollment_date = per.enrollment_date',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'ctb.consent_status = per.consent_status',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not specified',
            'transformation': 'ctb.source_system = per.source_system',
            'target_table': 'ctb'
        },
        {
            'source_column': "['per.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'sb.site_id = per.site_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'sb.trial_id = per.trial_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'sb.patient_id = per.patient_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'sb.patient_name = per.patient_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 'sb.gender = per.gender',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not specified',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not specified',
            'transformation': 'sb.date_of_birth = per.date_of_birth',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'sb.country = per.country',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'sb.enrollment_date = per.enrollment_date',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'sb.consent_status = per.consent_status',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not specified',
            'transformation': 'sb.source_system = per.source_system',
            'target_table': 'sb'
        },
        {
            'source_column': "['per.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not specified',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not specified',
            'transformation': 'cob.country = per.country',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'cob.trial_id = per.trial_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'cob.site_id = per.site_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'cob.patient_id = per.patient_id',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'cob.patient_name = per.patient_name',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not specified',
            'transformation': 'cob.gender = per.gender',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not specified',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not specified',
            'transformation': 'cob.date_of_birth = per.date_of_birth',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'cob.enrollment_date = per.enrollment_date',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not specified',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not specified',
            'transformation': 'cob.consent_status = per.consent_status',
            'target_table': 'cob'
        },
        {
            'source_column': "['per.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not specified',
            'transformation': 'cob.source_system = per.source_system',
            'target_table': 'cob'
        },
        {
            'source_column': "['wmr.device_record_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'device_record_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'mb.device_record_id = wmr.device_record_id',
            'target_table': 'mb'
        },
        {
            'source_column': "['wmr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'mb.patient_id = wmr.patient_id',
            'target_table': 'mb'
        },
        {
            'source_column': "['wmr.device_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'device_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'mb.device_type = wmr.device_type',
            'target_table': 'mb'
        },
        {
            'source_column': "['wmr.recorded_timestamp']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'recorded_timestamp',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'mb.recorded_timestamp = wmr.recorded_timestamp',
            'target_table': 'mb'
        },
        {
            'source_column': "['wmr.glucose_level']",
            'source_type': 'float',
            'source_nullable': 'not specified',
            'target_column': 'glucose_level',
            'target_type': 'float',
            'target_nullable': 'not specified',
            'transformation': 'mb.glucose_level = wmr.glucose_level',
            'target_table': 'mb'
        },
        {
            'source_column': "['wmr.step_count']",
            'source_type': 'int',
            'source_nullable': 'not specified',
            'target_column': 'step_count',
            'target_type': 'int',
            'target_nullable': 'not specified',
            'transformation': 'mb.step_count = wmr.step_count',
            'target_table': 'mb'
        },
        {
            'source_column': "['wmr.sleep_hours']",
            'source_type': 'float',
            'source_nullable': 'not specified',
            'target_column': 'sleep_hours',
            'target_type': 'float',
            'target_nullable': 'not specified',
            'transformation': 'mb.sleep_hours = wmr.sleep_hours',
            'target_table': 'mb'
        },
        {
            'source_column': "['wmr.heart_rate']",
            'source_type': 'float',
            'source_nullable': 'not specified',
            'target_column': 'heart_rate',
            'target_type': 'float',
            'target_nullable': 'not specified',
            'transformation': 'mb.heart_rate = wmr.heart_rate',
            'target_table': 'mb'
        },
        {
            'source_column': "['wmr.battery_status']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'battery_status',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'mb.battery_status = wmr.battery_status',
            'target_table': 'mb'
        },
        {
            'source_column': "['cvr.visit_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'visit_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dmb.visit_id = cvr.visit_id',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dmb.patient_id = cvr.patient_id',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.trial_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dmb.trial_id = cvr.trial_id',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.visit_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not specified',
            'target_column': 'visit_date',
            'target_type': 'timestamp',
            'target_nullable': 'not specified',
            'transformation': 'dmb.visit_date = cvr.visit_date',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.visit_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'visit_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dmb.visit_type = cvr.visit_type',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.blood_pressure']",
            'source_type': 'float64',
            'source_nullable': 'not specified',
            'target_column': 'blood_pressure',
            'target_type': 'float64',
            'target_nullable': 'not specified',
            'transformation': 'dmb.blood_pressure = cvr.blood_pressure',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.heart_rate']",
            'source_type': 'float64',
            'source_nullable': 'not specified',
            'target_column': 'heart_rate',
            'target_type': 'float64',
            'target_nullable': 'not specified',
            'transformation': 'dmb.heart_rate = cvr.heart_rate',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.weight_kg']",
            'source_type': 'float64',
            'source_nullable': 'not specified',
            'target_column': 'weight_kg',
            'target_type': 'float64',
            'target_nullable': 'not specified',
            'transformation': 'dmb.weight_kg = cvr.weight_kg',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.physician_notes']",
            'source_type': 'text',
            'source_nullable': 'not specified',
            'target_column': 'physician_notes',
            'target_type': 'text',
            'target_nullable': 'not specified',
            'transformation': 'dmb.physician_notes = cvr.physician_notes',
            'target_table': 'dmb'
        },
        {
            'source_column': "['cvr.source_system']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not specified',
            'target_column': 'source_system',
            'target_type': 'varchar(255)',
            'target_nullable': 'not specified',
            'transformation': 'dmb.source_system = cvr.source_system',
            'target_table': 'dmb'
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
    mapping_details = table['mapping_details']
    source_table = mapping_details.split()[0]
    source_alias = mapping_details.split()[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata['columns']:
        if col_meta['target_table'] == target_alias:
            transformation = col_meta['transformation']
            lhs, rhs = transformation.split("=", 1)
            rhs = rhs.strip()
            target_column = col_meta['target_column']
            transformations.append(f"{rhs} as {target_column}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_path + target_table + "." + write_format)

job.commit()