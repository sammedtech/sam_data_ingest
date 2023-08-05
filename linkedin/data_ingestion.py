import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RELEASE_TIMESTAMP", "INPUT_PATH", "OUTPUT_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

release_ts = args['RELEASE_TIMESTAMP']
input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']

linkedin_df = spark.read.csv(input_path, header=True)

linkedin_df.write.mode("overwrite").json(output_path + release_ts)

