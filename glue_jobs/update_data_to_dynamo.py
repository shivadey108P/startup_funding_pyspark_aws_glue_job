import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the source S3 path
s3_input_path = "s3://glue-poc-shivadey/processed_data/"

# ✅ Step 1: Read from S3 (Parquet format with Snappy compression)
s3_data_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="parquet",
    connection_options={"paths": [s3_input_path], "recurse": True},
    transformation_ctx="s3_data_dyf"
)

# ✅ Step 2: Write to DynamoDB table (startup_funding)
glueContext.write_dynamic_frame.from_options(
    frame=s3_data_dyf,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": "startup_funding",
        "dynamodb.throughput.write.percent": "1.0"
    },
    transformation_ctx="write_to_dynamodb"
)

job.commit()
