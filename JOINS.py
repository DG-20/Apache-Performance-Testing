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
    result = spark.sql(
        """
    SELECT r.*, b.authors
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    """
    )
    result.count()

    result = spark.sql(
        """
    SELECT r.Id, r.Title, b.authors, b.description
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE r.`review/score` >= 4 AND b.publishedDate > '2000'
    """
    )
    result.count()

    result = spark.sql(
        """
    SELECT b.categories, AVG(r.`review/score`) AS avg_rating, COUNT(DISTINCT b.Title) AS books_count
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    GROUP BY b.categories
    """
    )
    result.count()

    result = spark.sql(
        """
    SELECT r.Title, b.authors, r.`review/summary`, b.description
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE r.`review/text` LIKE '%excellent%' AND b.description IS NOT NULL
    """
    )
    result.count()

    result = spark.sql(
        """
    SELECT r.Title, b.authors, AVG(r.`review/score`) AS avg_score
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE b.authors LIKE '%Stephen King%'
    GROUP BY r.Title, b.authors
    ORDER BY avg_score DESC
    LIMIT 10
    """
    )

    result.count()

    result = spark.sql(
        """
    SELECT r.Title, r.`review/summary`, b.image
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE b.image IS NULL
    """
    )

    result.count()

    result = spark.sql(
        """
    SELECT r.Title, b.authors, b.publishedDate, r.`review/score`
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE r.`review/score` = 5 AND b.publishedDate < '2000'
    """
    )

    result.count()

    result = spark.sql(
        """
    SELECT r.Title, r.`review/summary`, b.infoLink
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE b.ratingsCount IS NULL
    """
    )

    result.count()

    result = spark.sql(
        """
    SELECT r.Title, b.categories, r.`review/score`, b.previewLink
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE b.categories LIKE '%Fiction%' AND r.`review/score` = 5
    """
    )

    result.count()

    result = spark.sql(
        """
    SELECT r.Title, b.authors, b.categories, r.`review/helpfulness`
    FROM reviews r
    JOIN books b ON r.Title = b.Title
    WHERE r.`review/helpfulness` LIKE '100%'
    ORDER BY r.`review/score` DESC
    """
    )

    result.count()

end_time = time.time()

print(f"JOIN Queries completed in {end_time - start_time} seconds.")
spark.stop()
