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
            'target_table': 'customers_bronze',
            'target_alias': 'cb',
            'mapping_details': 'customer_master cm',
            'description': "Bronze raw ingestion of customer attributes for the 'customers' silver entity; direct mapping from customer_master without joins or aggregations."
        },
        {
            'target_schema': 'bronze',
            'target_table': 'branches_bronze',
            'target_alias': 'bb',
            'mapping_details': 'branch_master bm',
            'description': "Bronze raw ingestion of branch attributes for the 'branches' silver entity; direct mapping from branch_master without joins or aggregations."
        },
        {
            'target_schema': 'bronze',
            'target_table': 'loan_applications_bronze',
            'target_alias': 'lab',
            'mapping_details': 'loan_applications la',
            'description': "Bronze raw ingestion of loan application records forming the core of the 'loans' silver entity; direct mapping from loan_applications without joins or aggregations."
        },
        {
            'target_schema': 'bronze',
            'target_table': 'repayment_transactions_bronze',
            'target_alias': 'rtb',
            'mapping_details': 'repayment_transactions rt',
            'description': "Bronze raw ingestion of repayment transaction events to support downstream loan repayment and balance calculations for the 'loans' silver entity; direct mapping from repayment_transactions without joins or aggregations."
        },
        {
            'target_schema': 'bronze',
            'target_table': 'loan_risk_assessment_bronze',
            'target_alias': 'lrab',
            'mapping_details': 'loan_risk_assessment lra',
            'description': "Bronze raw ingestion of loan risk assessment records to support downstream enrichment of the 'loans' silver entity; direct mapping from loan_risk_assessment without joins or aggregations."
        }
    ],
    'columns': [
        {
            'source_column': "['cm.customer_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'customer_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'cb.customer_id = cm.customer_id',
            'target_table': 'cb'
        },
        {
            'source_column': "['cm.customer_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'customer_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'cb.customer_name = cm.customer_name',
            'target_table': 'cb'
        },
        {
            'source_column': "['cm.gender']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'gender',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'cb.gender = cm.gender',
            'target_table': 'cb'
        },
        {
            'source_column': "['cm.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'cb.city = cm.city',
            'target_table': 'cb'
        },
        {
            'source_column': "['cm.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'cb.state = cm.state',
            'target_table': 'cb'
        },
        {
            'source_column': "['cm.income_segment']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'income_segment',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'cb.income_segment = cm.income_segment',
            'target_table': 'cb'
        },
        {
            'source_column': "['cm.annual_income']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'annual_income',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'cb.annual_income = cm.annual_income',
            'target_table': 'cb'
        },
        {
            'source_column': "['cm.credit_score']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'credit_score',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'cb.credit_score = cm.credit_score',
            'target_table': 'cb'
        },
        {
            'source_column': "['bm.branch_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'branch_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'bb.branch_id = bm.branch_id',
            'target_table': 'bb'
        },
        {
            'source_column': "['bm.branch_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'branch_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'bb.branch_name = bm.branch_name',
            'target_table': 'bb'
        },
        {
            'source_column': "['bm.city']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'city',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'bb.city = bm.city',
            'target_table': 'bb'
        },
        {
            'source_column': "['bm.state']",
            'source_type': 'varchar(100)',
            'source_nullable': 'accepted',
            'target_column': 'state',
            'target_type': 'varchar(100)',
            'target_nullable': 'accepted',
            'transformation': 'bb.state = bm.state',
            'target_table': 'bb'
        },
        {
            'source_column': "['bm.branch_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'accepted',
            'target_column': 'branch_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'accepted',
            'transformation': 'bb.branch_type = bm.branch_type',
            'target_table': 'bb'
        },
        {
            'source_column': "['bm.manager_name']",
            'source_type': 'varchar(255)',
            'source_nullable': 'accepted',
            'target_column': 'manager_name',
            'target_type': 'varchar(255)',
            'target_nullable': 'accepted',
            'transformation': 'bb.manager_name = bm.manager_name',
            'target_table': 'bb'
        },
        {
            'source_column': "['bm.opening_year']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'opening_year',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'bb.opening_year = bm.opening_year',
            'target_table': 'bb'
        },
        {
            'source_column': "['bm.contact_number']",
            'source_type': 'varchar(15)',
            'source_nullable': 'accepted',
            'target_column': 'contact_number',
            'target_type': 'varchar(15)',
            'target_nullable': 'accepted',
            'transformation': 'bb.contact_number = bm.contact_number',
            'target_table': 'bb'
        },
        {
            'source_column': "['la.loan_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'loan_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'lab.loan_id = la.loan_id',
            'target_table': 'lab'
        },
        {
            'source_column': "['la.customer_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'customer_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'lab.customer_id = la.customer_id',
            'target_table': 'lab'
        },
        {
            'source_column': "['la.branch_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'branch_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'lab.branch_id = la.branch_id',
            'target_table': 'lab'
        },
        {
            'source_column': "['la.loan_type']",
            'source_type': 'varchar(50)',
            'source_nullable': 'accepted',
            'target_column': 'loan_type',
            'target_type': 'varchar(50)',
            'target_nullable': 'accepted',
            'transformation': 'lab.loan_type = la.loan_type',
            'target_table': 'lab'
        },
        {
            'source_column': "['la.loan_amount']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'loan_amount',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'lab.loan_amount = la.loan_amount',
            'target_table': 'lab'
        },
        {
            'source_column': "['la.interest_rate']",
            'source_type': 'float',
            'source_nullable': 'accepted',
            'target_column': 'interest_rate',
            'target_type': 'float',
            'target_nullable': 'accepted',
            'transformation': 'lab.interest_rate = la.interest_rate',
            'target_table': 'lab'
        },
        {
            'source_column': "['la.loan_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'accepted',
            'target_column': 'loan_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'accepted',
            'transformation': 'lab.loan_status = la.loan_status',
            'target_table': 'lab'
        },
        {
            'source_column': "['la.application_date']",
            'source_type': 'date',
            'source_nullable': 'accepted',
            'target_column': 'application_date',
            'target_type': 'date',
            'target_nullable': 'accepted',
            'transformation': 'lab.application_date = la.application_date',
            'target_table': 'lab'
        },
        {
            'source_column': "['rt.transaction_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'not_accepted',
            'target_column': 'transaction_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'not_accepted',
            'transformation': 'rtb.transaction_id = rt.transaction_id',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rt.loan_id']",
            'source_type': 'varchar(20)',
            'source_nullable': 'accepted',
            'target_column': 'loan_id',
            'target_type': 'varchar(20)',
            'target_nullable': 'accepted',
            'transformation': 'rtb.loan_id = rt.loan_id',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rt.payment_date']",
            'source_type': 'date',
            'source_nullable': 'accepted',
            'target_column': 'payment_date',
            'target_type': 'date',
            'target_nullable': 'accepted',
            'transformation': 'rtb.payment_date = rt.payment_date',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rt.payment_amount']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'payment_amount',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'rtb.payment_amount = rt.payment_amount',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rt.payment_method']",
            'source_type': 'varchar(50)',
            'source_nullable': 'accepted',
            'target_column': 'payment_method',
            'target_type': 'varchar(50)',
            'target_nullable': 'accepted',
            'transformation': 'rtb.payment_method = rt.payment_method',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rt.payment_status']",
            'source_type': 'varchar(20)',
            'source_nullable': 'accepted',
            'target_column': 'payment_status',
            'target_type': 'varchar(20)',
            'target_nullable': 'accepted',
            'transformation': 'rtb.payment_status = rt.payment_status',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rt.remaining_balance']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'remaining_balance',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'rtb.remaining_balance = rt.remaining_balance',
            'target_table': 'rtb'
        },
        {
            'source_column': "['rt.processed_by']",
            'source_type': 'varchar(50)',
            'source_nullable': 'accepted',
            'target_column': 'processed_by',
            'target_type': 'varchar(50)',
            'target_nullable': 'accepted',
            'transformation': 'rtb.processed_by = rt.processed_by',
            'target_table': 'rtb'
        },
        {
            'source_column': "['lra.risk_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'not_accepted',
            'target_column': 'risk_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'not_accepted',
            'transformation': 'lrab.risk_id = lra.risk_id',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lra.loan_id']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'loan_id',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'lrab.loan_id = lra.loan_id',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lra.risk_score']",
            'source_type': 'int',
            'source_nullable': 'accepted',
            'target_column': 'risk_score',
            'target_type': 'int',
            'target_nullable': 'accepted',
            'transformation': 'lrab.risk_score = lra.risk_score',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lra.default_probability']",
            'source_type': 'double',
            'source_nullable': 'accepted',
            'target_column': 'default_probability',
            'target_type': 'double',
            'target_nullable': 'accepted',
            'transformation': 'lrab.default_probability = lra.default_probability',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lra.risk_category']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'risk_category',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'lrab.risk_category = lra.risk_category',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lra.evaluation_date']",
            'source_type': 'date',
            'source_nullable': 'accepted',
            'target_column': 'evaluation_date',
            'target_type': 'date',
            'target_nullable': 'accepted',
            'transformation': 'lrab.evaluation_date = lra.evaluation_date',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lra.analyst_name']",
            'source_type': 'varchar(50)',
            'source_nullable': 'accepted',
            'target_column': 'analyst_name',
            'target_type': 'varchar(50)',
            'target_nullable': 'accepted',
            'transformation': 'lrab.analyst_name = lra.analyst_name',
            'target_table': 'lrab'
        },
        {
            'source_column': "['lra.review_status']",
            'source_type': 'varchar(10)',
            'source_nullable': 'accepted',
            'target_column': 'review_status',
            'target_type': 'varchar(10)',
            'target_nullable': 'accepted',
            'transformation': 'lrab.review_status = lra.review_status',
            'target_table': 'lrab'
        }
    ],
    'runtime_config': {
        'base_path': 's3://sdlc-agent-bucket/engineering-agent/src/',
        'target_path': 's3://sdlc-agent-bucket/engineering-agent/bronze/',
        'read_format': 'csv',
        'write_format': 'csv',
        'write_mode': 'overwrite'
    }
}

base_path = metadata['runtime_config']['base_path']
target_path = metadata['runtime_config']['target_path']
read_format = metadata['runtime_config']['read_format']
write_format = metadata['runtime_config']['write_format']
write_mode = metadata['runtime_config']['write_mode']

for table in metadata['tables']:
    mapping_details = table['mapping_details'].split()
    source_table = mapping_details[0]
    source_alias = mapping_details[1]
    target_table = table['target_table']
    target_alias = table['target_alias']

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(base_path + source_table + "." + read_format)
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

    writer.save(target_path + target_table + "." + write_format)

job.commit()