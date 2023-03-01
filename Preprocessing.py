from pyspark.sql import SparkSession
from datetime import date

# Creating a spark session
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Reading the file into dataframe
filename = "balance_information.csv"
df = spark.read.options(header='true', inferSchema='true') \
    .csv(filename)

# Replacing null/no values with 0
df = df.fillna(value=0)

# Removing duplicate records
df = df.dropDuplicates()

# Removing rows with all values as 0
df.na.drop("all")

# Converting the spark dataframe to pandas dataframe
x = df.toPandas()

# Creating a file name with the actual file name and date appended to it
temp_name = filename.replace('.csv', '')
date = date.today()
#s = str(temp_name) + str(date) + '.csv'

# Creating a csv file with preprocessed data
x.to_csv(r'C:\temp\balance_information.csv', index=False)
