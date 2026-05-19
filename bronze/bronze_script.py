<GLUE_SCRIPT>
import sys
import os
import logging
import json
import datetime

from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.context import SparkContext

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'METADATA'])
meta = json.loads(args['METADATA'])
sc = SparkContext()
gc = GlueContext(sc)
spark = gc.spark_session
job = Job(gc)
job.init(args['JOB_NAME'], args)


def add_audit_columns(df, batch_id, source_system, source_path, partition_columns):
    df = (df
          .withColumn('_ingestion_timestamp', F.current_timestamp())
          .withColumn('_batch_id', F.lit(batch_id))
          .withColumn('_source_system', F.lit(source_system))
          .withColumn('_source_path', F.lit(source_path)))

    if '_ingestion_date' in partition_columns:
        df = df.withColumn('_ingestion_date', F.to_date(F.current_timestamp()).cast('string'))

    return df


for table in meta['tables']:
    try:
        source_table = table['mapping_details']['source_table']
        source_alias = table['mapping_details']['source_alias']
        target_table = table['target_table']
        partition_cols = table['partition_columns']

        source_path = meta['runtime_config']['base_path'] + "/" + source_table
        target_out_path = meta['runtime_config']['target_path'] + "/" + target_table

        df = (spark.read
              .format(meta['runtime_config']['read_format'])
              .options(**meta['runtime_config']['read_options'])
              .load(source_path))
        df = df.alias(source_alias)
        logger.info(f"READ  | table={source_table} | path={source_path}")

        matched_columns = [c for c in table['columns'] if c['target_alias'] == source_alias]
        if len(matched_columns) == 0:
            raise ValueError(f"No columns matched source_alias for table: {target_table}")

        df = df.selectExpr(*[c['transformation'] for c in matched_columns])

        df = add_audit_columns(
            df,
            batch_id=meta['job']['batch_id'],
            source_system=meta['job']['source_system'],
            source_path=source_path,
            partition_columns=partition_cols
        )

        writer = (df.write
                  .mode(meta['runtime_config']['write_mode'])
                  .format(meta['runtime_config']['write_format'])
                  .options(**meta['runtime_config']['write_options']))

        if len(partition_cols) > 0:
            writer = writer.partitionBy(*partition_cols)

        writer.save(target_out_path)
        logger.info(f"WRITE | table={target_table} | path={target_out_path}")

    except Exception as e:
        logger.error(f"FAILED | table={table.get('target_table')} | error={e}")
        raise

job.commit()
</GLUE_SCRIPT>