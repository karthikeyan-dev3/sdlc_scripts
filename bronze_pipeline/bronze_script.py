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
            'target_table': 'orders_bronze',
            'target_alias': 'ob',
            'mapping_details': 'transactions t',
            'description': 'Bronze orders table mapped 1:1 from transactions capturing order-level transaction details across POS and online channels.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'order_items_bronze',
            'target_alias': 'oib',
            'mapping_details': 'transaction_items ti',
            'description': 'Bronze order items table mapped 1:1 from transaction_items capturing product-level line items for each transaction.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'customers_bronze',
            'target_alias': 'cb',
            'mapping_details': 'customers c',
            'description': 'Bronze customers master table mapped 1:1 from customers containing customer profile and loyalty attributes.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'stores_bronze',
            'target_alias': 'sb',
            'mapping_details': 'stores s',
            'description': 'Bronze stores reference table mapped 1:1 from stores containing physical store metadata including region and store type.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'products_bronze',
            'target_alias': 'pb',
            'mapping_details': 'products p',
            'description': 'Bronze products catalog table mapped 1:1 from products containing product, category, subcategory, brand, pricing and status.'
        },
        {
            'target_schema': 'bronze',
            'target_table': 'campaign_events_bronze',
            'target_alias': 'ceb',
            'mapping_details': 'campaign_events ce',
            'description': 'Bronze campaign events table mapped 1:1 from campaign_events capturing marketing engagement and conversion signals at event level.'
        }
    ],
    'columns': [
        {
            'source_column': "['ob.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ob.transaction_id = t.transaction_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.customer_id']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'customer_id',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'ob.customer_id = t.customer_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'ob.store_id = t.store_id',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.transaction_datetime']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_null',
            'target_column': 'transaction_datetime',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_null',
            'transformation': 'ob.transaction_datetime = t.transaction_datetime',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.total_amount']",
            'source_type': 'DECIMAL(12,2)',
            'source_nullable': 'not_null',
            'target_column': 'total_amount',
            'target_type': 'DECIMAL(12,2)',
            'target_nullable': 'not_null',
            'transformation': 'ob.total_amount = t.total_amount',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.discount_amount']",
            'source_type': 'DECIMAL(10,2)',
            'source_nullable': 'nan',
            'target_column': 'discount_amount',
            'target_type': 'DECIMAL(10,2)',
            'target_nullable': 'nan',
            'transformation': 'ob.discount_amount = t.discount_amount',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.tax_amount']",
            'source_type': 'DECIMAL(10,2)',
            'source_nullable': 'nan',
            'target_column': 'tax_amount',
            'target_type': 'DECIMAL(10,2)',
            'target_nullable': 'nan',
            'transformation': 'ob.tax_amount = t.tax_amount',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.payment_method']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'payment_method',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'ob.payment_method = t.payment_method',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.channel']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'channel',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'ob.channel = t.channel',
            'target_table': 'ob'
        },
        {
            'source_column': "['ob.status']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'status',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'ob.status = t.status',
            'target_table': 'ob'
        },
        {
            'source_column': "['oib.transaction_item_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'transaction_item_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'oib.transaction_item_id = ti.transaction_item_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['oib.transaction_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'transaction_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'oib.transaction_id = ti.transaction_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['oib.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'oib.product_id = ti.product_id',
            'target_table': 'oib'
        },
        {
            'source_column': "['oib.quantity']",
            'source_type': 'INTEGER',
            'source_nullable': 'not_null',
            'target_column': 'quantity',
            'target_type': 'INTEGER',
            'target_nullable': 'not_null',
            'transformation': 'oib.quantity = ti.quantity',
            'target_table': 'oib'
        },
        {
            'source_column': "['oib.unit_price']",
            'source_type': 'DECIMAL(10,2)',
            'source_nullable': 'not_null',
            'target_column': 'unit_price',
            'target_type': 'DECIMAL(10,2)',
            'target_nullable': 'not_null',
            'transformation': 'oib.unit_price = ti.unit_price',
            'target_table': 'oib'
        },
        {
            'source_column': "['oib.total_price']",
            'source_type': 'DECIMAL(12,2)',
            'source_nullable': 'not_null',
            'target_column': 'total_price',
            'target_type': 'DECIMAL(12,2)',
            'target_nullable': 'not_null',
            'transformation': 'oib.total_price = ti.total_price',
            'target_table': 'oib'
        },
        {
            'source_column': "['cb.customer_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'customer_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'cb.customer_id = c.customer_id',
            'target_table': 'cb'
        },
        {
            'source_column': "['cb.first_name']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'first_name',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'cb.first_name = c.first_name',
            'target_table': 'cb'
        },
        {
            'source_column': "['cb.last_name']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'last_name',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'cb.last_name = c.last_name',
            'target_table': 'cb'
        },
        {
            'source_column': "['cb.gender']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'gender',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'cb.gender = c.gender',
            'target_table': 'cb'
        },
        {
            'source_column': "['cb.city']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'cb.city = c.city',
            'target_table': 'cb'
        },
        {
            'source_column': "['cb.state']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'cb.state = c.state',
            'target_table': 'cb'
        },
        {
            'source_column': "['cb.signup_date']",
            'source_type': 'DATE',
            'source_nullable': 'nan',
            'target_column': 'signup_date',
            'target_type': 'DATE',
            'target_nullable': 'nan',
            'transformation': 'cb.signup_date = c.signup_date',
            'target_table': 'cb'
        },
        {
            'source_column': "['cb.loyalty_tier']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'loyalty_tier',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'cb.loyalty_tier = c.loyalty_tier',
            'target_table': 'cb'
        },
        {
            'source_column': "['sb.store_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'store_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'sb.store_id = s.store_id',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_name']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'store_name',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'sb.store_name = s.store_name',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.city']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'city',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'sb.city = s.city',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.state']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'state',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'sb.state = s.state',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.region']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'region',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'sb.region = s.region',
            'target_table': 'sb'
        },
        {
            'source_column': "['sb.store_type']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'store_type',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'sb.store_type = s.store_type',
            'target_table': 'sb'
        },
        {
            'source_column': "['pb.product_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'product_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'pb.product_id = p.product_id',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.product_name']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'product_name',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'pb.product_name = p.product_name',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.category']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'category',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'pb.category = p.category',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.subcategory']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'subcategory',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'pb.subcategory = p.subcategory',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.brand']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'brand',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'pb.brand = p.brand',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.unit_cost']",
            'source_type': 'DECIMAL(10,2)',
            'source_nullable': 'nan',
            'target_column': 'unit_cost',
            'target_type': 'DECIMAL(10,2)',
            'target_nullable': 'nan',
            'transformation': 'pb.unit_cost = p.unit_cost',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.unit_price']",
            'source_type': 'DECIMAL(10,2)',
            'source_nullable': 'nan',
            'target_column': 'unit_price',
            'target_type': 'DECIMAL(10,2)',
            'target_nullable': 'nan',
            'transformation': 'pb.unit_price = p.unit_price',
            'target_table': 'pb'
        },
        {
            'source_column': "['pb.status']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'status',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'pb.status = p.status',
            'target_table': 'pb'
        },
        {
            'source_column': "['ceb.event_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'event_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ceb.event_id = ce.event_id',
            'target_table': 'ceb'
        },
        {
            'source_column': "['ceb.campaign_id']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'campaign_id',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ceb.campaign_id = ce.campaign_id',
            'target_table': 'ceb'
        },
        {
            'source_column': "['ceb.customer_id']",
            'source_type': 'STRING',
            'source_nullable': 'nan',
            'target_column': 'customer_id',
            'target_type': 'STRING',
            'target_nullable': 'nan',
            'transformation': 'ceb.customer_id = ce.customer_id',
            'target_table': 'ceb'
        },
        {
            'source_column': "['ceb.event_type']",
            'source_type': 'STRING',
            'source_nullable': 'not_null',
            'target_column': 'event_type',
            'target_type': 'STRING',
            'target_nullable': 'not_null',
            'transformation': 'ceb.event_type = ce.event_type',
            'target_table': 'ceb'
        },
        {
            'source_column': "['ceb.event_timestamp']",
            'source_type': 'TIMESTAMP',
            'source_nullable': 'not_null',
            'target_column': 'event_timestamp',
            'target_type': 'TIMESTAMP',
            'target_nullable': 'not_null',
            'transformation': 'ceb.event_timestamp = ce.event_timestamp',
            'target_table': 'ceb'
        },
        {
            'source_column': "['ceb.conversion_flag']",
            'source_type': 'BOOLEAN',
            'source_nullable': 'nan',
            'target_column': 'conversion_flag',
            'target_type': 'BOOLEAN',
            'target_nullable': 'nan',
            'transformation': 'ceb.conversion_flag = ce.conversion_flag',
            'target_table': 'ceb'
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

runtime_config = metadata.get('runtime_config', {})
base_path = runtime_config.get('base_path')
target_path = runtime_config.get('target_path')
read_format = runtime_config.get('read_format')
write_format = runtime_config.get('write_format')
write_mode = runtime_config.get('write_mode')

for table in metadata.get('tables', []):
    mapping_details = table.get('mapping_details', '')
    parts = mapping_details.split()
    source_table = parts[0] if len(parts) > 0 else None
    source_alias = parts[1] if len(parts) > 1 else None

    target_table = table.get('target_table')
    target_alias = table.get('target_alias')

    reader = spark.read.format(read_format)
    if read_format == 'csv':
        reader = reader.option('header', 'true').option('inferSchema', 'true')

    df = reader.load(f"{base_path}{source_table}.{read_format}")
    df = df.alias(source_alias)

    transformations = []
    for col_meta in metadata.get('columns', []):
        if col_meta.get('target_table') == target_alias:
            transformation = col_meta.get('transformation', '')
            rhs = transformation.split('=', 1)[1].strip() if '=' in transformation else transformation
            target_col = col_meta.get('target_column')
            transformations.append(f"{rhs} as {target_col}")

    df = df.selectExpr(*transformations)

    writer = df.write.mode(write_mode).format(write_format)
    if write_format == 'csv':
        writer = writer.option('header', 'true')

    writer.save(f"{target_path}{target_table}.{write_format}")

job.commit()
