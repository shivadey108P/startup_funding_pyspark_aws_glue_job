from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_replace,
    lower,
    trim,
    when,
    sum as _sum
)
import os

# Initialize Spark session
spark = SparkSession.builder.appName("Clean Startup Funding").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load the CSV
csv_path = os.path.abspath(os.path.join(os.getcwd(), "..", "data", "startup_funding.csv"))

df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

# Standardize column names
df = df \
    .withColumnRenamed("Sr No", "sr_no") \
    .withColumnRenamed("Date dd/mm/yyyy", "date") \
    .withColumnRenamed("Startup Name", "startup_name") \
    .withColumnRenamed("Industry Vertical", "industry_v") \
    .withColumnRenamed("SubVertical", "sub_vertical") \
    .withColumnRenamed("City  Location", "city_loc") \
    .withColumnRenamed("Investors Name", "investor_name") \
    .withColumnRenamed("InvestmentnType", "investment_type") \
    .withColumnRenamed("Amount in USD", "amt_in_usd") \
    .withColumnRenamed("Remarks", "remarks")

# Clean and normalize startup name
df = df.withColumn("startup_name", lower(trim(col("startup_name"))))

# Handle known encoding artifacts in startup names (like \xe2\x80\x99)
df = df.withColumn("startup_name", regexp_replace("startup_name", r"\\x[a-zA-Z0-9]{2}", ""))  # removes \x.. hex
df = df.withColumn("startup_name", regexp_replace("startup_name", r"[^\x00-\x7F]", ""))  # removes non-ASCII chars
df = df.withColumn("startup_name", regexp_replace("startup_name", r"[\"']", ""))  # removes quotes/apostrophes

# Clean amount column
df = df.withColumn("amt_in_usd", regexp_replace("amt_in_usd", ",", ""))  # remove commas
df = df.withColumn("amt_in_usd", regexp_replace("amt_in_usd", r"[^\d]", ""))  # remove non-digit characters
df = df.withColumn("amt_in_usd", when(col("amt_in_usd") == "", None).otherwise(col("amt_in_usd")))
df = df.withColumn("amt_in_usd", col("amt_in_usd").cast("long"))

# Show cleaned data (optional)
print("\n📊 Cleaned Data Sample:")
df.select("sr_no", "date", "startup_name", "amt_in_usd").show(10, truncate=False)

# Aggregate: total funding per startup
agg_df = df.groupBy("startup_name").agg(_sum("amt_in_usd").alias("total_funding_usd"))

# Sort in descending order of funding
agg_df = agg_df.orderBy(col("total_funding_usd").desc_nulls_last())

print("\n💰 Total Funding per Startup:")
agg_df.show(20, truncate=False)

# Optional: save cleaned data if needed
# agg_df.write.mode("overwrite").csv("output/startup_funding_cleaned", header=True)
