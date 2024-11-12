from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType
from pyspark.sql import functions as psf

# Create a Spark session
spark = SparkSession.builder.appName("Sales Data Analysis").getOrCreate()

# Read the CSV file into a DataFrame
sales_df = spark.read.csv("salesData.csv", sep=';', header=True, inferSchema=True)
# sales_df.show()

# Define the schema
schema = StructType([
    StructField("Region", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Item Type", StringType(), True),
    StructField("Sales Channel", StringType(), True),
    StructField("Order Priority", StringType(), True),
    StructField("Order Date", DateType(), True),  # Note: Ensure date format is handled correctly
    StructField("Order ID", LongType(), True),
    StructField("Units Sold", DoubleType(), True),
    StructField("Unit Price", DoubleType(), True),
    StructField("Total Revenue", DoubleType(), True),
    StructField("Total Profit", DoubleType(), True)])


"""
To convert the Order Date column to date format as it showing 

# <!-- SparkUpgradeException: [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a 
different result due to the upgrading to Spark >= 3.0:
# Caused by: DateTimeParseException: Text '5/28/2010' could not be parsed at index 0 -->
"""

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# Derivation of orders
df_derive = sales_df.withColumn("Order ID", psf.col("Order ID").cast("long")) \
    .withColumn("Order Date", psf.to_timestamp(psf.regexp_replace(psf.col("Order Date"), "-", "/"), "MM/dd/yyyy").cast('date'))
# df_derive.show()

print("Calculate below aggregations")
print("1.1 Total revenue per region")
print("-------------------------------")
total_revenue_per_region = df_derive.groupBy("Region").agg(psf.sum("Total Revenue").alias("Total Revenue Per Region"))
total_revenue_per_region.show()

print("1.2 Top 5 countries where highest Household units are sold")
print("-------------------------------")
top_5_unit_sold = df_derive.filter(psf.col("Item Type") == "Household").groupBy("Country").agg(
    psf.sum("Units Sold").alias("TotalUnitSoldForCountry")).orderBy(psf.col("TotalUnitSoldForCountry").desc()).limit(5)
top_5_unit_sold.show()

print("1.3 Total profile in 'Asia' region from 2011 to 2015")
print("-------------------------------")
asia_total_profile = df_derive.filter((psf.col("Region") == "Asia") &
                                      (psf.year(psf.col("Order Date")).between(2011, 2015)))
asia_total_profile.show()

# write the output to CSV file
df = asia_total_profile.write.format('csv').save('sales/sales_output.csv', header=True, inferSchema=True)

