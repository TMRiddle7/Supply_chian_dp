import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv,['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'],args)

input_path = 's3://supply-chain-gw-proj-dev/raw/'
df = spark.read.option("header",True).json(input_path)

df = df.withColumn('ingest_date', col('ingest_ts').substr(1,10))

output_path = 's3://supply-chain-gw-proj-dev/bronze/'
df.write.mode('overwrite').parquet(output_path)

job.commit()