from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType
from pyspark.sql import functions as psf


class CustomerData:
    """
    Created CustomerData Class
    """
    def __init__(self, customerId: str, forename: str, surname: str):
        self.customerId = customerId
        self.forename = forename
        self.surname = surname

class AccountData:
    """
    Created AccountData Class
    """
    def __init__(self, customerId: str, accountId: str, balance: int):
        self.customerId = customerId
        self.accountId = accountId
        self.balance = balance

# Create a Spark session
spark = SparkSession.builder.appName("Customer Account Data Analysis").getOrCreate()

# Read the customer_data CSV file into a DataFrame
customer_data = spark.read.format('csv').load('customer_data.csv', header=True, inferSchema=True)
# customer_data.show()
customer_data.createOrReplaceTempView("customers")

# Read the account_data CSV file into a DataFrame
account_data = spark.read.format('csv').load('account_data.csv', header=True, inferSchema=True)
# account_data.show()
account_data.createOrReplaceTempView("accounts")

print("3. Analise and aggregate below queries:")
print("3.1 For each customer find total number of accounts associated")
df_join_grouped = account_data.groupBy("customerId").agg(psf.count("accountId").alias("NoOfAccounts"))
# df_join_grouped.show()


print("3.2 Find customers who has more than 2 accounts output should include customerId,forename,surname,NoOfacounts")
df_customer_morethan_2_account = df_join_grouped.filter(psf.col("NoOfAccounts") > 2)
df_join2 = customer_data.join(df_customer_morethan_2_account,
                              on=customer_data.customerId == df_customer_morethan_2_account.customerId, how="inner")\
    .drop(df_customer_morethan_2_account["customerId"])#.select("customerId", "forename", "surname", "NoOfAccounts")
# df_join2.show()

print("3.3 Display top 5 customer with highest account balance")
top_5_customers = account_data.select("customerId", "accountId", "balance")\
    .orderBy(psf.col("balance").desc()).limit(5)
# top_5_customers.show()