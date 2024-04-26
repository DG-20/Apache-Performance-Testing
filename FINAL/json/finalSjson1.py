from pyspark.sql import SparkSession
from pyspark import SparkConf
import time
import logging
import psutil
import math

# Setting up logging
logging.basicConfig(filename='imdb_select1_performance.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Initialize Spark Session
conf = SparkConf().setAppName("533 Spark SQL SELECT Performance Test").set("spark.ui.port", "4051")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Function to log CPU and Memory Usage
def log_system_resources():
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    logging.info(f"CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")

# Function to execute and time SQL queries, log system resources, and count results
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

# Load the dataset
df = spark.read.option("multiline", "true").json("part-01.json")
df.createOrReplaceTempView("reviews")

# Define SELECT queries
queries = [
    """SELECT movie, COUNT(review_id) AS review_count FROM reviews GROUP BY movie ORDER BY review_count DESC LIMIT 10""",
    """SELECT movie, AVG(rating) AS average_rating FROM reviews WHERE rating IS NOT NULL GROUP BY movie HAVING COUNT(review_id) > 100 ORDER BY average_rating DESC""",
    """SELECT reviewer, COUNT(review_id) AS reviews_written FROM reviews GROUP BY reviewer ORDER BY reviews_written DESC LIMIT 10""",
    """SELECT review_date, COUNT(review_id) AS daily_reviews FROM reviews GROUP BY review_date ORDER BY review_date""",
    """SELECT movie, COUNT(review_id) AS spoiler_count FROM reviews WHERE spoiler_tag = 1 GROUP BY movie ORDER BY spoiler_count DESC LIMIT 10""",
    """SELECT reviewer, movie, review_detail FROM reviews WHERE rating >= 8 AND LENGTH(review_detail) > 100 LIMIT 10""",
    """SELECT movie, AVG(helpful[0]/helpful[1]) AS avg_helpfulness FROM reviews WHERE size(helpful) = 2 AND helpful[1] != 0 GROUP BY movie HAVING COUNT(review_id) > 50 ORDER BY avg_helpfulness DESC""",
    """SELECT movie, reviewer, rating, review_summary FROM reviews WHERE rating IN (1, 10) LIMIT 10""",
    """SELECT movie, review_detail FROM reviews WHERE review_detail LIKE '%excellent%' LIMIT 10""",
    """SELECT rating, COUNT(review_id) AS number_of_reviews FROM reviews GROUP BY rating ORDER BY rating"""
]

overall_start_time = time.time()

# Execute only the first query, ensure no further execution
print("Starting execution of the first query")
execute_select_query(queries[0], print_results=True)

overall_end_time = time.time()
print(f"Total execution time: {overall_end_time - overall_start_time} seconds")

# Stop the Spark session
spark.stop()
