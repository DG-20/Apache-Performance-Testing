from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import time
import logging
import psutil
import math

# Setting up logging
logging.basicConfig(filename='spark_join50_performance.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Initialize Spark Session
conf = SparkConf().setAppName("533 Spark SQL JOIN Performance Test").set("spark.ui.port", "4051")
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
    """
    SELECT r.*, b.authors
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    """,
    """
    SELECT r.Id, r.Title, b.authors, b.description
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE r.`review/score` >= 4 AND b.publishedDate > '2000'
    """,
    """
    SELECT b.categories, AVG(r.`review/score`) AS avg_rating, COUNT(DISTINCT b.Title) AS books_count
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    GROUP BY b.categories
    """,
    """
    SELECT r.Title, b.authors, r.`review/summary`, b.description
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE r.`review/text` LIKE '%excellent%' AND b.description IS NOT NULL
    """,
    """
    SELECT r.Title, b.authors, AVG(r.`review/score`) AS avg_score
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE b.authors LIKE '%Stephen King%'
    GROUP BY r.Title, b.authors
    ORDER BY avg_score DESC
    LIMIT 10
    """,
    """
    SELECT r.Title, r.`review/summary`, b.image
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE b.image IS NULL
    """,
    """
    SELECT r.Title, b.authors, b.publishedDate, r.`review/score`
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE r.`review/score` = 5 AND b.publishedDate < '2000'
    """,
    """
    SELECT r.Title, r.`review/summary`, b.infoLink
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE b.ratingsCount IS NULL
    """,
    """
    SELECT r.Title, b.categories, r.`review/score`, b.previewLink
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE b.categories LIKE '%Fiction%' AND r.`review/score` = 5
    """,
    """
    SELECT r.Title, b.authors, b.categories, r.`review/helpfulness`
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE r.`review/helpfulness` LIKE '100%'
    ORDER BY r.`review/score` DESC
    """
]
overall_start_time = time.time()

resp_times = []

# Execute each query 5 times (5 epochs)
for epoch in range(5):
    print(f"Starting epoch {epoch+1}")
    ovr_start_time = time.time()
    for i, query in enumerate(queries):
        
        if epoch == 0 and i == 0:  # Check if it's the first query of the first epoch
            execute_join_query(query, print_results=True)
        else:
            execute_join_query(query)
        
    resp_times.append(time.time() - ovr_start_time)

mean_resp_time = sum(resp_times)/len(resp_times)

sample_variance_squared_value = 0
for resp_time in resp_times:
    sample_variance_squared_value += resp_time ** 2

sample_variance = math.sqrt((sample_variance_squared_value - (5 * (mean_resp_time ** 2)))/4)

plus_minus_num = 2.7764*(sample_variance)/math.sqrt(5)

print(f"JOIN queries' 95% confidence interval for response times is ({mean_resp_time - plus_minus_num}, {mean_resp_time + plus_minus_num}) seconds, with mean {mean_resp_time} seconds.")

overall_end_time = time.time()
print(f"Total execution time: {overall_end_time - overall_start_time} seconds")
# Stop the Spark session
spark.stop()