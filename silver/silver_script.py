import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# NOTE:
# Per UDT config, none of the requested silver_* clinical/patient tables
# can be built from the provided bronze domain (POS sales, payments,
# inventory, footfall). No valid source tables/columns are provided.
# Therefore, no reads, transforms, or writes are performed to avoid
# inventing tables/columns not present in the UDT.
# -------------------------------------------------------------------

job.commit()