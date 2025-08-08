import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Read from raw folder in S3
raw_data_path = "s3://glue-poc-shivadey/raw/"

raw_data_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [raw_data_path], "recurse": True},
    transformation_ctx="raw_data_dyf"
)

# Evaluate data quality
EvaluateDataQuality().process_rows(
    frame=raw_data_dyf,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQualityContext",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# Write to archive folder in S3 as Parquet (Snappy compression works well with Parquet)
archive_data_path = "s3://glue-poc-shivadey/archive/"

glueContext.write_dynamic_frame.from_options(
    frame=raw_data_dyf,
    connection_type="s3",
    format="parquet",  # Changed from CSV to Parquet for Snappy support
    connection_options={"path": archive_data_path},
    format_options={"compression": "snappy"},
    transformation_ctx="write_to_archive"
)

job.commit()
