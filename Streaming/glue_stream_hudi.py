# streaming/glue_stream_hudi.py (simplified example)
from pyspark.sql import SparkSession
hudi_options = {
'hoodie.table.name': 'events_hudi',
'hoodie.datasource.write.recordkey.field': 'event_id',
'hoodie.datasource.write.precombine.field': 'ingest_ts',
'hoodie.datasource.write.partitionpath.field': 'ingest_date',
'hoodie.datasource.write.operation': 'upsert',
'hoodie.datasource.write.table.type': 'COPY_ON_WRITE'
}
spark = SparkSession.builder.appName('hudi_upsert').getOrCreate()
bronze_path = 's3://supply-chain-gw-proj-dev/bronze/'
hudi_path = 's3://supply-chain-gw-proj-dev/silver/hudi/events_hudi'
df = spark.read.parquet(bronze_path)
(df.write
.format('org.apache.hudi')
.options(**hudi_options)
.mode('append')
.save(hudi_path))
spark.stop()