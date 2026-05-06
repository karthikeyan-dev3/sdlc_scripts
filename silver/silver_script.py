```python
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, row_number, coalesce
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ETL").getOrCreate()
glueContext = GlueContext(spark.sparkContext)

# Read and transform department_dim_silver
department_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/department_curated_reference.csv/")
department_df.createOrReplaceTempView("departments")

department_dim_silver_sql = """
SELECT 
    department_id,
    department_code,
    department_name,
    department_group,
    is_active,
    effective_start_time,
    effective_end_time,
    updated_at
FROM (
    SELECT 
        department_id,
        department_code,
        department_name,
        department_group,
        is_active,
        effective_start_time,
        effective_end_time,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY department_code ORDER BY effective_start_time DESC, updated_at DESC) rn
    FROM departments
) 
WHERE rn = 1
"""
department_dim_silver_df = spark.sql(department_dim_silver_sql)
department_dim_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/department_dim_silver.csv", header=True)

# Read and transform bed_dim_silver
bed_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/bed_curated_reference.csv/")
bed_df.createOrReplaceTempView("beds")

bed_dim_silver_sql = """
SELECT 
    b.bed_id,
    dd.department_id,
    dd.department_name AS department,
    b.bed_type,
    b.is_active,
    b.effective_start_time,
    b.effective_end_time,
    b.updated_at
FROM 
    beds b
INNER JOIN 
    department_dim_silver dd
ON 
    b.department_code = dd.department_code
"""
bed_dim_silver_df = spark.sql(bed_dim_silver_sql)
bed_dim_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/bed_dim_silver.csv", header=True)

# Read and transform resource_dim_silver
resource_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/resource_curated_reference.csv/")
resource_df.createOrReplaceTempView("resources")

resource_dim_silver_sql = """
SELECT 
    r.resource_id,
    r.resource_type,
    coalesce(dd.department_id, bd.department_id) AS department_id,
    coalesce(dd.department_name, bd.department) AS department,
    r.is_active,
    r.updated_at
FROM 
    resources r
LEFT JOIN 
    department_dim_silver dd
ON 
    r.department_code = dd.department_code
LEFT JOIN 
    bed_dim_silver bd
ON 
    r.resource_id = bd.bed_id
"""
resource_dim_silver_df = spark.sql(resource_dim_silver_sql)
resource_dim_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/resource_dim_silver.csv", header=True)

# Read and transform patient_encounter_silver
encounter_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/patient_encounters.csv/")
encounter_df.createOrReplaceTempView("encounters")

patient_encounter_silver_sql = """
SELECT 
    e.patient_id,
    e.admission_time,
    CASE WHEN e.discharge_time >= e.admission_time THEN e.discharge_time ELSE NULL END AS discharge_time,
    dd.department_id,
    dd.department_name AS department,
    e.bed_id,
    CAST((UNIX_TIMESTAMP(e.discharge_time) - UNIX_TIMESTAMP(e.admission_time)) / 60 AS INT) AS time_in_department_minutes,
    CASE WHEN e.discharge_time >= e.admission_time THEN 1 ELSE 0 END AS record_valid_flag,
    e.updated_at
FROM 
    encounters e
INNER JOIN 
    department_dim_silver dd
ON 
    e.department_code = dd.department_code
LEFT JOIN 
    bed_dim_silver bd
ON 
    e.bed_id = bd.bed_id
"""
patient_encounter_silver_df = spark.sql(patient_encounter_silver_sql)
patient_encounter_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/patient_encounter_silver.csv", header=True)

# Read and transform resource_allocation_silver
allocation_df = spark.read.format("csv").option("header", "true").load(f"s3://sdlc-agent-bucket/engineering-agent/bronze/resource_allocations.csv/")
allocation_df.createOrReplaceTempView("allocations")

resource_allocation_silver_sql = """
SELECT 
    a.resource_id,
    rd.department_id,
    rd.department,
    a.allocation_start_time,
    CASE WHEN a.allocation_end_time >= a.allocation_start_time THEN a.allocation_end_time ELSE NULL END AS allocation_end_time,
    CAST((UNIX_TIMESTAMP(a.allocation_end_time) - UNIX_TIMESTAMP(a.allocation_start_time)) / 60 AS INT) AS allocation_duration_minutes,
    CASE WHEN a.allocation_end_time >= a.allocation_start_time THEN 1 ELSE 0 END AS record_valid_flag,
    a.updated_at
FROM 
    allocations a
INNER JOIN 
    resource_dim_silver rd
ON 
    a.resource_id = rd.resource_id
"""
resource_allocation_silver_df = spark.sql(resource_allocation_silver_sql)
resource_allocation_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/resource_allocation_silver.csv", header=True)

# Read and transform operational_daily_silver
operational_daily_silver_sql = """
SELECT 
    CAST(pe.admission_time AS DATE) AS date,
    AVG(NULLIF(pe.time_in_department_minutes, 0)) AS avg_wait_time_minutes,
    SUM(pe.time_in_department_minutes) / (COUNT(DISTINCT pe.bed_id) * 24 * 60) AS bed_utilization_rate,
    MAX(pe.updated_at) AS dashboard_update_time_seconds,
    MAX(pe.updated_at) AS updated_at
FROM 
    patient_encounter_silver pe
LEFT JOIN 
    resource_allocation_silver ra 
ON 
    DATE(pe.admission_time) = DATE(ra.allocation_start_time)
GROUP BY 
    DATE(pe.admission_time)
"""
operational_daily_silver_df = spark.sql(operational_daily_silver_sql)
operational_daily_silver_df.write.mode("overwrite").csv("s3://sdlc-agent-bucket/engineering-agent/silver/operational_daily_silver.csv", header=True)
```