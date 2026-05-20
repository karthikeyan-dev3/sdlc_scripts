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
            'mapping_details': 'patient_enrollment_raw_2000 pe',
            'description': 'Bronze ingestion of patient enrollment records for downstream patient_analytics, dashboard_reporting, and data_governance. Columns mapped from patient_enrollment_raw_2000: patient_id, trial_id, site_id, patient_name, gender, date_of_birth, country, enrollment_date, consent_status, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'clinical_visits_bronze',
            'target_alias': 'cvb',
            'mapping_details': 'clinical_visit_raw_2000 cv',
            'description': 'Bronze ingestion of clinical visit records for downstream clinical_visits, patient_analytics, dashboard_reporting, and data_governance. Columns mapped from clinical_visit_raw_2000: visit_id, patient_id, trial_id, visit_date, visit_type, blood_pressure, heart_rate, weight_kg, physician_notes, source_system.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'lab_results_bronze',
            'target_alias': 'lrb',
            'mapping_details': 'lab_results_raw_2000 lr',
            'description': 'Bronze ingestion of lab result records for downstream lab_results, patient_analytics, dashboard_reporting, and data_governance. Columns mapped from lab_results_raw_2000: lab_result_id, patient_id, sample_id, test_name, test_result, test_unit, reference_range, abnormal_flag, test_date, lab_name.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'drug_administration_bronze',
            'target_alias': 'dab',
            'mapping_details': 'drug_administration_raw_2000 da',
            'description': 'Bronze ingestion of drug administration records for downstream treatment_analysis, patient_analytics, dashboard_reporting, and data_governance. Columns mapped from drug_administration_raw_2000: administration_id, patient_id, drug_code, dosage_mg, administration_date, administration_route, administered_by, batch_number.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'adverse_events_bronze',
            'target_alias': 'aeb',
            'mapping_details': 'adverse_events_raw_2000 ae',
            'description': 'Bronze ingestion of adverse event records for downstream treatment_analysis, patient_analytics, dashboard_reporting, and data_governance. Columns mapped from adverse_events_raw_2000: event_id, patient_id, event_type, severity, event_start_date, event_end_date, outcome, related_to_drug, hospitalization_required, reported_by.'
        }
    ],
    'columns': [
        {
            'source_column': "['peb.patient_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'peb.patient_id = pe.patient_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.trial_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'peb.trial_id = pe.trial_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.site_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'site_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'peb.site_id = pe.site_id',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.patient_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'patient_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'peb.patient_name = pe.patient_name',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'peb.gender = pe.gender',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.date_of_birth']",
            'source_type': 'date',
            'source_nullable': 'not_specified',
            'target_column': 'date_of_birth',
            'target_type': 'date',
            'target_nullable': 'not_specified',
            'transformation': 'peb.date_of_birth = pe.date_of_birth',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.country']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'country',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'peb.country = pe.country',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.enrollment_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not_specified',
            'target_column': 'enrollment_date',
            'target_type': 'timestamp',
            'target_nullable': 'not_specified',
            'transformation': 'peb.enrollment_date = pe.enrollment_date',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.consent_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'consent_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'peb.consent_status = pe.consent_status',
            'target_table': 'peb'
        },
        {
            'source_column': "['peb.source_system']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_specified',
            'target_column': 'source_system',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_specified',
            'transformation': 'peb.source_system = pe.source_system',
            'target_table': 'peb'
        },
        {
            'source_column': "['cvb.visit_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'visit_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.visit_id = cv.visit_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.patient_id = cv.patient_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.trial_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'trial_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.trial_id = cv.trial_id',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.visit_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not_specified',
            'target_column': 'visit_date',
            'target_type': 'timestamp',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.visit_date = cv.visit_date',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.visit_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'visit_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.visit_type = cv.visit_type',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.blood_pressure']",
            'source_type': 'double',
            'source_nullable': 'not_specified',
            'target_column': 'blood_pressure',
            'target_type': 'double',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.blood_pressure = cv.blood_pressure',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.heart_rate']",
            'source_type': 'double',
            'source_nullable': 'not_specified',
            'target_column': 'heart_rate',
            'target_type': 'double',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.heart_rate = cv.heart_rate',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.weight_kg']",
            'source_type': 'double',
            'source_nullable': 'not_specified',
            'target_column': 'weight_kg',
            'target_type': 'double',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.weight_kg = cv.weight_kg',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.physician_notes']",
            'source_type': 'text',
            'source_nullable': 'not_specified',
            'target_column': 'physician_notes',
            'target_type': 'text',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.physician_notes = cv.physician_notes',
            'target_table': 'cvb'
        },
        {
            'source_column': "['cvb.source_system']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'source_system',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'cvb.source_system = cv.source_system',
            'target_table': 'cvb'
        },
        {
            'source_column': "['lrb.lab_result_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'lab_result_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.lab_result_id = lr.lab_result_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.patient_id = lr.patient_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.sample_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'sample_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.sample_id = lr.sample_id',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.test_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'test_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.test_name = lr.test_name',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.test_result']",
            'source_type': 'double',
            'source_nullable': 'not_specified',
            'target_column': 'test_result',
            'target_type': 'double',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.test_result = lr.test_result',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.test_unit']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'test_unit',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.test_unit = lr.test_unit',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.reference_range']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'reference_range',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.reference_range = lr.reference_range',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.abnormal_flag']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'abnormal_flag',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.abnormal_flag = lr.abnormal_flag',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.test_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not_specified',
            'target_column': 'test_date',
            'target_type': 'timestamp',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.test_date = lr.test_date',
            'target_table': 'lrb'
        },
        {
            'source_column': "['lrb.lab_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'lab_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'lrb.lab_name = lr.lab_name',
            'target_table': 'lrb'
        },
        {
            'source_column': "['dab.administration_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'administration_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'dab.administration_id = da.administration_id',
            'target_table': 'dab'
        },
        {
            'source_column': "['dab.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'dab.patient_id = da.patient_id',
            'target_table': 'dab'
        },
        {
            'source_column': "['dab.drug_code']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'drug_code',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'dab.drug_code = da.drug_code',
            'target_table': 'dab'
        },
        {
            'source_column': "['dab.dosage_mg']",
            'source_type': 'double',
            'source_nullable': 'not_specified',
            'target_column': 'dosage_mg',
            'target_type': 'double',
            'target_nullable': 'not_specified',
            'transformation': 'dab.dosage_mg = da.dosage_mg',
            'target_table': 'dab'
        },
        {
            'source_column': "['dab.administration_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not_specified',
            'target_column': 'administration_date',
            'target_type': 'timestamp',
            'target_nullable': 'not_specified',
            'transformation': 'dab.administration_date = da.administration_date',
            'target_table': 'dab'
        },
        {
            'source_column': "['dab.administration_route']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'administration_route',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'dab.administration_route = da.administration_route',
            'target_table': 'dab'
        },
        {
            'source_column': "['dab.administered_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'administered_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'dab.administered_by = da.administered_by',
            'target_table': 'dab'
        },
        {
            'source_column': "['dab.batch_number']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'batch_number',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'dab.batch_number = da.batch_number',
            'target_table': 'dab'
        },
        {
            'source_column': "['aeb.event_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'event_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.event_id = ae.event_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.patient_id']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'patient_id',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.patient_id = ae.patient_id',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.event_type']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'event_type',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.event_type = ae.event_type',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.severity']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'severity',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.severity = ae.severity',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.event_start_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not_specified',
            'target_column': 'event_start_date',
            'target_type': 'timestamp',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.event_start_date = ae.event_start_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.event_end_date']",
            'source_type': 'timestamp',
            'source_nullable': 'not_specified',
            'target_column': 'event_end_date',
            'target_type': 'timestamp',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.event_end_date = ae.event_end_date',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.outcome']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'outcome',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.outcome = ae.outcome',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.related_to_drug']",
            'source_type': 'boolean',
            'source_nullable': 'not_specified',
            'target_column': 'related_to_drug',
            'target_type': 'boolean',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.related_to_drug = ae.related_to_drug',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.hospitalization_required']",
            'source_type': 'boolean',
            'source_nullable': 'not_specified',
            'target_column': 'hospitalization_required',
            'target_type': 'boolean',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.hospitalization_required = ae.hospitalization_required',
            'target_table': 'aeb'
        },
        {
            'source_column': "['aeb.reported_by']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'reported_by',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'aeb.reported_by = ae.reported_by',
            'target_table': 'aeb'
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
    mapping_details = table.get('mapping_details', '').split()
    source_table = mapping_details[0] if len(mapping_details) > 0 else None
    source_alias = mapping_details[1] if len(mapping_details) > 1 else None

    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    source_input_path = f"{base_path}{source_table}.{read_format}"

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(source_input_path)
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

    target_output_path = f"{target_path}{target_table}.{write_format}"

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option("header", "true")

    writer.save(target_output_path)

job.commit()

```