
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import time
import logging
import psutil
import math

# Setting up logging
logging.basicConfig(filename='spark_select1_performance.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Initialize Spark Session
conf = SparkConf().setAppName("533 Spark SQL SELECT Performance Test").set("spark.ui.port", "4052")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Function to log CPU and Memory Usage
def log_system_resources():
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    logging.info(f"CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")

# Function to execute and time SQL queries, log system resources, count and optionally print results
def execute_select_query(query, print_results=False):
    start_time = time.time()
    result = spark.sql(query)
    count = result.count()  # Trigger action and get count
    end_time = time.time()
    logging.info(f"SELECT Query executed in {end_time - start_time} seconds with {count} results")
    log_system_resources()
    if print_results:
        result.show()  # Show results if flag is True
    print(f"Result count for query: {count}")

# Load the datasets
df = spark.read.option("header", "true").option("inferSchema", "true").csv("Books_rating.csv")
df2 = spark.read.option("header", "true").option("inferSchema", "true").csv("books_data.csv")

df = df.withColumn("review/score", df["review/score"].cast("double"))
df = df.withColumn("price", df["price"].cast("double"))
df = df.withColumn("review/helpfulness", df["review/helpfulness"].cast("double"))
df = df.withColumn("review/time", df["review/time"].cast("timestamp"))

df.printSchema()
df.createOrReplaceTempView("reviews")
df2.createOrReplaceTempView("books")

# Define queries
queries = [
    "SELECT * FROM reviews WHERE `review/score` = 5.0",
    "SELECT title, price FROM reviews WHERE price < 20",
    "SELECT id, `review/helpfulness` FROM reviews ORDER BY `review/helpfulness` DESC",
    "SELECT `review/summary`, `review/text` FROM reviews WHERE `review/time` > '1577837521'",
    "SELECT * FROM reviews WHERE user_id = 'AZ0IOBU20TBOP'",
    "SELECT `review/score`, COUNT(*) as review_count FROM reviews GROUP BY `review/score`",
    "SELECT `review/score`, AVG(price) as average_price FROM reviews GROUP BY `review/score`",
    "SELECT title FROM reviews WHERE `review/summary` LIKE '%mystery%'",
    "SELECT user_id, profileName FROM reviews WHERE `review/score` = 1",
    "SELECT title, `review/text` FROM reviews WHERE `review/text` LIKE '%excellent%'"
]
overall_start_time = time.time()

# Execute only the first query, ensure no further execution
print("Starting execution of the first query")
execute_select_query(queries[0], print_results=True)

overall_end_time = time.time()
print(f"Total execution time: {overall_end_time - overall_start_time} seconds")
# Stop the Spark session
spark.stop()
