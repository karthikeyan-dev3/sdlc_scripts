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
            'target_table': 'branch_master_bronze',
            'target_alias': 'bmb',
            'mapping_details': 'branch_master bm',
            'description': 'Bronze table for raw branch data sourced from branch_master. Columns: branch_id, branch_name, city, state, branch_type, manager_name, opening_year, contact_number.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customer_master_bronze',
            'target_alias': 'cmb',
            'mapping_details': 'customer_master cm',
            'description': 'Bronze table for raw customer data sourced from customer_master. Columns: customer_id, customer_name, gender, city, state, income_segment, annual_income, credit_score.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'loan_applications_bronze',
            'target_alias': 'lab',
            'mapping_details': 'loan_applications la',
            'description': 'Bronze table for raw loan application data sourced from loan_applications. Columns: loan_id, customer_id, branch_id, loan_type, loan_amount, interest_rate, loan_status, application_date.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'loan_risk_assessment_bronze',
            'target_alias': 'lrab',
            'mapping_details': 'loan_risk_assessment lra',
            'description': 'Bronze table for raw loan risk assessment data sourced from loan_risk_assessment. Columns: risk_id, loan_id, risk_score, default_probability, risk_category, evaluation_date, analyst_name, review_status.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'repayment_transactions_bronze',
            'target_alias': 'rtb',
            'mapping_details': 'repayment_transactions rt',
            'description': 'Bronze table for raw repayment transaction data sourced from repayment_transactions. Columns: transaction_id, loan_id, payment_date, payment_amount, payment_method, payment_status, remaining_balance, processed_by.'
        }
    ],
    'columns': [
        {
            'source_column': "['bmb.branch_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'branch_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'bmb.branch_id = bm.branch_id',
            'target_table': 'bmb'
        },
        {
            'source_column': "['bmb.branch_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'branch_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'bmb.branch_name = bm.branch_name',
            'target_table': 'bmb'
        },
        {
            'source_column': "['bmb.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'bmb.city = bm.city',
            'target_table': 'bmb'
        },
        {
            'source_column': "['bmb.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'bmb.state = bm.state',
            'target_table': 'bmb'
        },
        {
            'source_column': "['bmb.branch_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_specified',
            'target_column': 'branch_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_specified',
            'transformation': 'bmb.branch_type = bm.branch_type',
            'target_table': 'bmb'
        },
        {
            'source_column': "['bmb.manager_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'manager_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'bmb.manager_name = bm.manager_name',
            'target_table': 'bmb'
        },
        {
            'source_column': "['bmb.opening_year']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'opening_year',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'bmb.opening_year = bm.opening_year',
            'target_table': 'bmb'
        },
        {
            'source_column': "['bmb.contact_number']",
            'source_type': 'varchar(15)',
            'source_nullable': 'not_specified',
            'target_column': 'contact_number',
            'target_type': 'varchar(15)',
            'target_nullable': 'not_specified',
            'transformation': 'bmb.contact_number = bm.contact_number',
            'target_table': 'bmb'
        },
        {
            'source_column': "['cmb.customer_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'customer_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'cmb.customer_id = cm.customer_id',
            'target_table': 'cmb'
        },
        {
            'source_column': "['cmb.customer_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'not_specified',
            'target_column': 'customer_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'not_specified',
            'transformation': 'cmb.customer_name = cm.customer_name',
            'target_table': 'cmb'
        },
        {
            'source_column': "['cmb.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'cmb.gender = cm.gender',
            'target_table': 'cmb'
        },
        {
            'source_column': "['cmb.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'cmb.city = cm.city',
            'target_table': 'cmb'
        },
        {
            'source_column': "['cmb.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'not_specified',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'not_specified',
            'transformation': 'cmb.state = cm.state',
            'target_table': 'cmb'
        },
        {
            'source_column': "['cmb.income_segment']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'income_segment',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'cmb.income_segment = cm.income_segment',
            'target_table': 'cmb'
        },
        {
            'source_column': "['cmb.annual_income']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'annual_income',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'cmb.annual_income = cm.annual_income',
            'target_table': 'cmb'
        },
        {
            'source_column': "['cmb.credit_score']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'credit_score',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'cmb.credit_score = cm.credit_score',
            'target_table': 'cmb'
        },
        {
            'source_column': "['lab.loan_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'loan_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'lab.loan_id = la.loan_id',
            'target_table': 'lab'
        },
        {
            'source_column': "['lab.customer_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'customer_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'lab.customer_id = la.customer_id',
            'target_table': 'lab'
        },
        {
            'source_column': "['lab.branch_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'branch_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'lab.branch_id = la.branch_id',
            'target_table': 'lab'
        },
        {
            'source_column': "['lab.loan_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_specified',
            'target_column': 'loan_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_specified',
            'transformation': 'lab.loan_type = la.loan_type',
            'target_table': 'lab'
        },
        {
            'source_column': "['lab.loan_amount']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'loan_amount',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'lab.loan_amount = la.loan_amount',
            'target_table': 'lab'
        },
        {
            'source_column': "['lab.interest_rate']",
            'source_type': 'float',
            'source_nullable': 'not_specified',
            'target_column': 'interest_rate',
            'target_type': 'float',
            'target_nullable': 'not_specified',
            'transformation': 'lab.interest_rate = la.interest_rate',
            'target_table': 'lab'
        },
        {
            'source_column': "['lab.loan_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'loan_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'lab.loan_status = la.loan_status',
            'target_table': 'lab'
        },
        {
            'source_column': "['lab.application_date']",
            'source_type': 'date',
            'source_nullable': 'not_specified',
            'target_column': 'application_date',
            'target_type': 'date',
            'target_nullable': 'not_specified',
            'transformation': 'lab.application_date = la.application_date',
            'target_table': 'lab'
        },
        {
            'source_column': "['lrab.risk_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'risk_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'lrab.risk_id = lra.risk_id',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lrab.loan_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'loan_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'lrab.loan_id = lra.loan_id',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lrab.risk_score']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'risk_score',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'lrab.risk_score = lra.risk_score',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lrab.default_probability']",
            'source_type': 'double',
            'source_nullable': 'not_specified',
            'target_column': 'default_probability',
            'target_type': 'double',
            'target_nullable': 'not_specified',
            'transformation': 'lrab.default_probability = lra.default_probability',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lrab.risk_category']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'risk_category',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'lrab.risk_category = lra.risk_category',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lrab.evaluation_date']",
            'source_type': 'date',
            'source_nullable': 'not_specified',
            'target_column': 'evaluation_date',
            'target_type': 'date',
            'target_nullable': 'not_specified',
            'transformation': 'lrab.evaluation_date = lra.evaluation_date',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lrab.analyst_name']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_specified',
            'target_column': 'analyst_name',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_specified',
            'transformation': 'lrab.analyst_name = lra.analyst_name',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lrab.review_status']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_specified',
            'target_column': 'review_status',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_specified',
            'transformation': 'lrab.review_status = lra.review_status',
            'target_table': 'lrab'
        },
        {
            'source_column': "['rtb.transaction_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'transaction_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'rtb.transaction_id = rt.transaction_id',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rtb.loan_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'loan_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'rtb.loan_id = rt.loan_id',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rtb.payment_date']",
            'source_type': 'date',
            'source_nullable': 'not_specified',
            'target_column': 'payment_date',
            'target_type': 'date',
            'target_nullable': 'not_specified',
            'transformation': 'rtb.payment_date = rt.payment_date',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rtb.payment_amount']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'payment_amount',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'rtb.payment_amount = rt.payment_amount',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rtb.payment_method']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'payment_method',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'rtb.payment_method = rt.payment_method',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rtb.payment_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_specified',
            'target_column': 'payment_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_specified',
            'transformation': 'rtb.payment_status = rt.payment_status',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rtb.remaining_balance']",
            'source_type': 'int',
            'source_nullable': 'not_specified',
            'target_column': 'remaining_balance',
            'target_type': 'int',
            'target_nullable': 'not_specified',
            'transformation': 'rtb.remaining_balance = rt.remaining_balance',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rtb.processed_by']",
            'source_type': 'varchar(50)',
            'source_nullable': 'not_specified',
            'target_column': 'processed_by',
            'target_type': 'varchar(50)',
            'target_nullable': 'not_specified',
            'transformation': 'rtb.processed_by = rt.processed_by',
            'target_table': 'rtb'
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

    df = reader.load(base_path + source_table + "." + read_format)
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else ''
            target_col = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(target_path + target_table + "." + write_format)

job.commit()
