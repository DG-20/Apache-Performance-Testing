from pyspark.sql import SparkSession
import time

# Initialize Spark Session
spark = SparkSession.builder.appName("533 Spark SQL Performance Test").getOrCreate()

# Get the SparkContext from SparkSession
sc = spark.sparkContext

# Set log level
sc.setLogLevel("WARN")  # You can use ERROR, WARN, INFO, DEBUG

# Load the dataset
df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("Books_rating.csv")
)

df2 = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("books_data.csv")
)

df = df.withColumn("review/score", df["review/score"].cast("double"))
df = df.withColumn("price", df["price"].cast("double"))
df = df.withColumn("review/helpfulness", df["review/helpfulness"].cast("double"))
df = df.withColumn("review/time", df["review/time"].cast("timestamp"))

df.printSchema()

df.createOrReplaceTempView("reviews")
df2.createOrReplaceTempView("books")

start_time = time.time()

# Epochs.
for i in range(1):
    result = spark.sql("SELECT * FROM reviews WHERE `review/score` = 5.0")
    result.count()
    result = spark.sql("SELECT title, price FROM reviews WHERE price < 20")
    result.count()
    result = spark.sql(
        "SELECT id, `review/helpfulness` FROM reviews ORDER BY `review/helpfulness` DESC"
    )
    result.count()
    result = spark.sql(
        "SELECT `review/summary`, `review/text` FROM reviews WHERE `review/time` > '1577837521'"
    )
    result.count()
    result = spark.sql("SELECT * FROM reviews WHERE user_id = 'AZ0IOBU20TBOP'")
    result.count()
    result = spark.sql(
        "SELECT `review/score`, COUNT(*) as review_count FROM reviews GROUP BY `review/score`"
    )
    result.count()
    result = spark.sql(
        "SELECT `review/score`, AVG(price) as average_price FROM reviews GROUP BY `review/score`"
    )
    result.count()
    result = spark.sql(
        "SELECT title FROM reviews WHERE `review/summary` LIKE '%mystery%'"
    )
    result.count()
    result = spark.sql(
        "SELECT user_id, profileName FROM reviews WHERE `review/score` = 1"
    )
    result.count()
    result = spark.sql(
        "SELECT title, `review/text` FROM reviews WHERE `review/text` LIKE '%excellent%'"
    )
    result.count()

end_time = time.time()

print(f"SELECT Queries completed in {end_time - start_time} seconds.")
spark.stop()
