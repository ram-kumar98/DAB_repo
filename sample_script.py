from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum

# Create Spark session
spark = SparkSession.builder \
    .appName("SimplePySparkNotebook") \
    .getOrCreate()

print("Spark session created successfully!")
print(f"Spark version: {spark.version}")


# Create a simple DataFrame
data = [
    ("Alice", "Sales", 5000, 28),
    ("Bob", "Marketing", 6000, 35),
    ("Charlie", "Sales", 5500, 30),
    ("Diana", "IT", 7000, 32),
    ("Eve", "Marketing", 5200, 29),
    ("Frank", "IT", 7500, 40),
    ("Grace", "Sales", 4800, 26)
]

columns = ["Name", "Department", "Salary", "Age"]

df = spark.createDataFrame(data, columns)

# Show the data
print("Original Data:")
df.show()

spark.stop()


