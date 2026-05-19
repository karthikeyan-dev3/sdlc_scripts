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


def add_audit_columns(df, batch_id, source_system, layer, partition_columns):
    df_out = (df
              .withColumn("_ingestion_timestamp", F.current_timestamp())
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_source_system", F.lit(source_system))
              .withColumn("_layer", F.lit(layer)))

    if "_ingestion_date" in (partition_columns or []):
        df_out = df_out.withColumn("_ingestion_date", F.to_date(F.current_timestamp()).cast("string"))

    return df_out


for table in meta['tables']:
    target_table = None
    try:
        target_table = table['target_table']
        source_tables = table['source_tables']
        sql_query = table['sql_query']
        partition_cols = table['partition_columns']
        dedup_conf = table.get('dedup', None)

        for entry in source_tables:
            src_path = meta['runtime_config']['source_path'] + "/" + entry['table_name']
            df_src = (spark.read
                      .format(meta['runtime_config']['read_format'])
                      .options(**meta['runtime_config']['read_options'])
                      .load(src_path))
            logger.info(f"READ  | table={entry['table_name']} | path={src_path}")
            df_src.createOrReplaceTempView(entry['alias'])

        df = spark.sql(sql_query)

        if dedup_conf is not None and dedup_conf['enabled'] is True:
            from pyspark.sql import Window  # imported only when needed for dedup; per spec

            window_spec = (Window
                           .partitionBy(*dedup_conf['partition_by'])
                           .orderBy(F.expr(dedup_conf['order_by'])))

            df = df.withColumn(dedup_conf['row_number_col'], F.row_number().over(window_spec))
            df = df.filter(F.col(dedup_conf['row_number_col']) == 1).drop(dedup_conf['row_number_col'])

        df = add_audit_columns(
            df,
            batch_id=meta['job']['batch_id'],
            source_system=meta['job']['source_system'],
            layer=meta['layer'],
            partition_columns=partition_cols
        )

        target_out_path = meta['runtime_config']['target_path'] + "/" + target_table

        writer = (df.write
                  .mode(meta['runtime_config']['write_mode'])
                  .format(meta['runtime_config']['write_format'])
                  .options(**meta['runtime_config']['write_options']))

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(target_out_path)
        logger.info(f"WRITE | table={target_table} | path={target_out_path}")

    except Exception as e:
        logger.error(f"FAILED | table={target_table} | error={e}")
        raise

job.commit()
</GLUE_SCRIPT>