from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, concat_ws, collect_list
from pyspark.sql.types import BooleanType
import os


# Data Imputation and Transformation Pipeline
"""
The techniques involve in  preparing modeling data, including splitting data, handling missing values, encoding categorical features,
and standardizing features. It includes outlier removal and coercing columns to the correct data type.
By the end, it will give a comprehensive understanding of data preparation for modeling and feature preparation.
"""
spark = SparkSession.builder \
    .appName("Data Imputation and Transformation Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

file_path = r"C:\Users\mural\Downloads\telco-customer-churn-noisy.csv"

# Data Cleaning and Imputation
"""
- Load the dataset from the specified path using Spark and read it as a DataFrame.
- Drop any rows with missing values from the DataFrame using the dropna() method.
- Fill any remaining missing values in the DataFrame with the 0 using the fillna() method.
- Create a temporary view named as telco_customer_churn
"""

telco_noisy_df = spark.read.csv(file_path, header="true", inferSchema="true", multiLine="true", escape='"')
# print(telco_noisy_df.count())
# telco_noisy_df.show()
# print(telco_noisy_df.printSchema())


# Coerce/Fix Data Types
"""
- Even though most of the data types are correct let's do the following to have a better memory footprint of the dataframe in memory
- Convert SeniorCitizen and Churn binary columns to boolean type.
- Converting the tenure column to a long integer using .selectExpr and reordering the columns.
- Using spark.sql to convert Partner , Dependents, PhoneService and PaperlessBilling columns to boolean, and reordering the columns. Then, saving the dataframe as a DELTA table.
"""
binary_columns = ["SeniorCitizen", "Churn"]
for column in binary_columns:
    telco_customer_noisy_churn_df = telco_noisy_df.withColumn(column, col(column).cast(BooleanType()))

# Enable the below printSchema after you cast the "binary_columns" to boolean
# telco_customer_noisy_churn_df.select(*binary_columns).printSchema()

# if the casting didn't work on "SeniorCitizen" - most probably because of null values exist in the column, so we can force Coerce to fix it
telco_customer_noisy_churn_df = telco_customer_noisy_churn_df.withColumn("SeniorCitizen",
                                                                             when(col("SeniorCitizen") == 1, True)
                                                                             .otherwise(False))
# telco_customer_noisy_churn_df.select("SeniorCitizen").printSchema()

telco_customer_noisy_churn_df.createOrReplaceTempView("telco_customer_churn_temp_view")

# Columns you already casted and listed
cast_columns = ["Dependents", "Partner", "PhoneService", "PaperlessBilling", 'tenure']

# All columns in the DataFrame
all_columns = telco_customer_noisy_churn_df.columns

# Columns to include after excluding cast_columns plus 'customerID' and 'Churn'
select_columns = [col for col in all_columns if col not in cast_columns + ["customerID", "Churn"]]

# Manually build the SQL query string
telco_customer_casted_df = spark.sql("""
SELECT
  customerID,
  CAST(Dependents AS BOOLEAN) AS Dependents,
  CAST(Partner AS BOOLEAN) AS Partner,
  CAST(PhoneService AS BOOLEAN) AS PhoneService,
  CAST(PaperlessBilling AS BOOLEAN) AS PaperlessBilling,
  CAST(tenure AS LONG) AS tenure,
  {}
  , Churn
FROM telco_customer_churn_temp_view
""".format(", ".join(select_columns)))


# telco_customer_casted_df.select("Dependents", "Partner", "PaperlessBilling", "PhoneService", "tenure").printSchema()


 # ------------------------------------------------------------------------------------------------- #
 # Handling Outliers
"""
- How to handle outliers in column by identifying and addressing data points that fall far outside the typical range of values in a dataset. 
- Common methods for handling outliers include removing them, filtering, transforming the data, or replacing outliers with more representative values.

Follow these steps for handling outliers:
- Create a new silver table named as telco_customer_full_silver by appending silver to the original table name and then accessing it using Spark SQL.
- Filtering out outliers from the TotalCharges column by removing rows where the column value exceeds the specified cutoff value.
"""
telco_customer_full_silver = "delta_tables/telco_customer_full_silver"

# Create full path relative to current working directory
telco_customer_full_silver_path = os.path.join(os.getcwd(), telco_customer_full_silver)
print(telco_customer_full_silver_path)

# Write Delta table to the relative path
try:
    telco_customer_casted_df.write.mode('overwrite').option("mergeSchema",True).save(telco_customer_full_silver_path)
    print("The output is written as delta format and saved it output folder")
except Exception as e:
    print(f'The data is not written {e} ')

# ------------------------------------------------------------------------------------------------- #

# Filtering out outliers from the TotalCharges column by removing rows where the column value exceeds the specified cutoff value (e.g. negative values
telco_customer_casted_df.select('TotalCharges', 'tenure')
# Remove customers with negative TotalCharges
total_charges_cutoff = 0
telco_no_outliers_df = telco_customer_casted_df.filter((col("TotalCharges") > total_charges_cutoff) | (col("TotalCharges").isNull())) # Keep Nulls

# Removing outliers from PaymentMethod
"""
- Identify the two lowest occurrence groups in the PaymentMethod column and calculating the total count and average MonthlyCharges for each group.
- Removing customers from the identified low occurrence groups in the PaymentMethod column to filter out outliers.
- Create a new dataframe telco_filtered_df containing the filtered data.
- Comparing the count of records before and after by dividing the count of telco_casted_full_df and telco_no_outliers_df dataframe removing outliers and then materializing the resulting dataframe as a new table.
"""
stats_df = telco_no_outliers_df.groupBy("PaymentMethod")\
                                .agg(count("*").alias("Total"),\
                                     avg("MonthlyCharges").alias('MonthlyCharger'))\
                                .orderBy(col("Total").desc())
# stats_df.show()

# Gather 2 groups with the lowest counts assuming the count threshold is below 20% of the full dataset and monthly charges < $70
n = telco_no_outliers_df.count()

lower_groups = [elem["PaymentMethod"] if elem["PaymentMethod"] is not None else "null"
                for elem in stats_df.tail(2) if elem['Total']/n < 0.2 and elem['MonthlyCharger'] < 70]

print(f"Removing groups: {','.join(lower_groups)}")

# Filter/Remove listings from these low occurrence groups while keeping null occurrences
telco_no_outliers_df = telco_no_outliers_df.filter(~col("PaymentMethod").isin(lower_groups) | col("PaymentMethod").isNull())

# Count/Compare datasets before/after removing outliers
print(f"Count - Before: {telco_customer_casted_df.count()} / After: {telco_no_outliers_df.count()}")
try:
    telco_no_outliers_df.write.mode('overwrite').option("mergeSchema",True).save(os.path.join(telco_customer_full_silver + "_outliers"))
    print("The output is written for detecting outliers as delta format and saved it output folder")
except Exception as e:
    print(f'The data is not written {e} ')

# ------------------------------------------------------------------------------------------------- #
