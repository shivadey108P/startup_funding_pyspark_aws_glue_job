import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lower, trim, regexp_replace, when
from awsglue.gluetypes import *

def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str):
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

# Glue job setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw data from S3
raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_options={"paths": ["s3://glue-poc-shivadey/raw_data/"], "recurse": True},
    transformation_ctx="raw_dyf"
)

# Drop null fields (optional cleanup)
cleaned_dyf = drop_nulls(glueContext, frame=raw_dyf, nullStringSet={}, nullIntegerSet={}, transformation_ctx="cleaned_dyf")

# Convert to Spark DataFrame for transformations
df = cleaned_dyf.toDF()

# Standardize column names
df = df \
    .withColumnRenamed("Sr No", "sr_no") \
    .withColumnRenamed("Startup Name", "startup_name") \
    .withColumnRenamed("Date dd/mm/yyyy", "date") \
    .withColumnRenamed("Industry Vertical", "industry_v") \
    .withColumnRenamed("SubVertical", "sub_vertical") \
    .withColumnRenamed("City  Location", "city_loc") \
    .withColumnRenamed("Investors Name", "investor_name") \
    .withColumnRenamed("InvestmentnType", "investment_type") \
    .withColumnRenamed("Amount in USD", "amt_in_usd") \
    .withColumnRenamed("Remarks", "remarks")

# Normalize startup_name (lowercase, trim, remove unwanted characters)
df = df.withColumn("startup_name", lower(trim(col("startup_name"))))
df = df.withColumn("startup_name", regexp_replace("startup_name", r"\\x[a-zA-Z0-9]{2}", ""))  # remove hex
df = df.withColumn("startup_name", regexp_replace("startup_name", r"[^\x00-\x7F]", ""))  # remove non-ASCII
df = df.withColumn("startup_name", regexp_replace("startup_name", r"[\"']", ""))  # remove quotes

# Clean amount field
df = df.withColumn("amt_in_usd", regexp_replace("amt_in_usd", ",", ""))
df = df.withColumn("amt_in_usd", regexp_replace("amt_in_usd", r"[^\d]", ""))  # remove non-digits
df = df.withColumn("amt_in_usd", when(col("amt_in_usd") == "", None).otherwise(col("amt_in_usd")))
df = df.withColumn("amt_in_usd", col("amt_in_usd").cast("long"))

# Convert back to Glue DynamicFrame
final_dyf = DynamicFrame.fromDF(df, glueContext, "final_dyf")

# Evaluate data quality
EvaluateDataQuality().process_rows(
    frame=final_dyf,
    ruleset="""
        Rules = [
            ColumnCount > 0
        ]
    """,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQualityContext",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# Write cleaned data to S3 processed location
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://glue-poc-shivadey/processed_data/", "partitionKeys": []},
    transformation_ctx="write_to_processed"
)

job.commit()
