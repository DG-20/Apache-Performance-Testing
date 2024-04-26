from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import time
import logging
import psutil
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr

import math

# Setting up logging
logging.basicConfig(filename='spark_imdb_self_join1_performance.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Initialize Spark Session
conf = SparkConf().setAppName("IMDb Spark SQL Self JOIN Performance Test").set("spark.ui.port", "4051")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Function to log CPU and Memory Usage
def log_system_resources():
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    logging.info(f"CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")

# Function to execute and time SQL queries, log system resources, count and optionally print results
def execute_join_query(query, print_results=False):
    start_time = time.time()
    result = spark.sql(query)
    count = result.count()  # Trigger action and get count
    end_time = time.time()
    logging.info(f"JOIN Query executed in {end_time - start_time} seconds with {count} results")
    log_system_resources()
    if print_results:
        result.show()  # Show results if flag is True
    print(f"Result count for query: {count}")

# Load the JSON dataset
df_reviews = spark.read.option("multiline", "true").json("part-01.json")

# Schema adjustments
df_reviews = df_reviews.withColumn("rating", df_reviews["rating"].cast("double"))
df_reviews = df_reviews.withColumn("review_date", F.to_date(df_reviews["review_date"], "yyyy-MM-dd"))
df_reviews = df_reviews.withColumn(
    "helpful",
    expr("""
        transform(helpful, item -> array(
            cast(regexp_extract(item, '(\\d+) people find the review helpful out of', 1) as int),
            cast(regexp_extract(item, 'out of (\\d+)', 1) as int)
        ))
    """)
)
df_reviews = df_reviews.withColumn("spoiler_tag", df_reviews["spoiler_tag"].cast("integer"))


# Register DataFrame as a temp view
df_reviews.createOrReplaceTempView("reviews")

# Define queries that use self-joins
queries = [
    """
    -- Reviews by the same reviewer on different movies
    SELECT a.reviewer, a.movie AS movie_a, b.movie AS movie_b, a.rating AS rating_a, b.rating AS rating_b
    FROM reviews a
    JOIN reviews b ON a.reviewer = b.reviewer AND a.movie != b.movie
    WHERE a.rating >= 8 AND b.rating >= 8
    LIMIT 10
    """,
    """
    -- Reviews on the same movie with differing spoiler tags
    SELECT a.movie, a.review_summary AS review_a, b.review_summary AS review_b, a.spoiler_tag, b.spoiler_tag
    FROM reviews a
    JOIN reviews b ON a.movie = b.movie AND a.review_id != b.review_id
    WHERE a.spoiler_tag != b.spoiler_tag
    LIMIT 10
    """,
    """
    -- Reviews comparing ratings on the same movie
    SELECT a.movie, a.review_id AS review_a, b.review_id AS review_b, a.rating AS rating_a, b.rating AS rating_b
    FROM reviews a
    JOIN reviews b ON a.movie = b.movie AND a.review_id != b.review_id
    WHERE ABS(a.rating - b.rating) > 2
    LIMIT 10
    """,
    """
    -- Comparing review summaries for the same movie on the same date
    SELECT a.movie, a.review_date, a.review_summary AS summary_a, b.review_summary AS summary_b
    FROM reviews a
    JOIN reviews b ON a.movie = b.movie AND a.review_date = b.review_date AND a.review_id != b.review_id
    LIMIT 10
    """,
    """
    -- Reviews by the same reviewer comparing ratings for movies with spoilers vs. non-spoilers
    SELECT a.reviewer, a.movie AS movie_a, b.movie AS movie_b, a.rating AS rating_a, b.rating AS rating_b, a.spoiler_tag AS spoiler_a, b.spoiler_tag AS spoiler_b
    FROM reviews a
    JOIN reviews b ON a.reviewer = b.reviewer AND a.spoiler_tag != b.spoiler_tag
    WHERE a.rating > 7 AND b.rating > 7
    LIMIT 10
    """
]

print("Showing helpful column structure:")
df_reviews.select("helpful").show(truncate=False)
df_reviews.printSchema()

overall_start_time = time.time()

# resp_times = []

# Execute first query
for epoch in range(1):
    print(f"Starting epoch {epoch+1}")
    for i, query in enumerate(queries):
        if i == 0:  # Execute only the first query
            execute_join_query(query, print_results=True)
        else:
            break  # Stop after the first query

# mean_resp_time = sum(resp_times)/len(resp_times)
# sample_variance_squared_value = 0
# for resp_time in resp_times:
    # sample_variance_squared_value += resp_time ** 2

# sample_variance = math.sqrt((sample_variance_squared_value - (5 * (mean_resp_time ** 2)))/4)

# plus_minus_num = 2.7764*(sample_variance)/math.sqrt(5)

# print(f"JOIN queries' 95% confidence interval for response times is ({mean_resp_time - plus_minus_num}, {mean_resp_time + plus_minus_num}) seconds, with mean {mean_resp_time} seconds.")

overall_end_time = time.time()
print(f"Total execution time: {overall_end_time - overall_start_time} seconds")

# Stop the Spark session
spark.stop()
