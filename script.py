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
for i in range(5):
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

print(f"Result completed in {end_time - start_time} seconds.")

# start_time = time.time()

# for i in range(100):
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

# # # result.show()

# end_time = time.time()

# # # result.show()
# print(f"Result completed in {end_time - start_time} seconds.")

# spark.sql(
#     """
# INSERT INTO reviews (Id, Title, User_id, `review/score`, `review/summary`, `review/text`)
# VALUES ('123456789', 'New Book Title', 'User123', 5, 'A Great Read', 'I thoroughly enjoyed this book because...')
# """
# )

# spark.sql(
#     """
# INSERT INTO books (Title, authors, description, publishedDate)
# VALUES ('New Book Title', 'Author Name', 'This is a description of the new book.', '2024-01-01')
# """
# )

# spark.sql(
#     """
# INSERT INTO reviews (Id, Title, User_id, `review/score`, `review/summary`, `review/text`)
# VALUES
# ('234567890', 'Another Book', 'User456', 4, 'Good, but not great', 'The book was overall enjoyable, however...'),
# ('345678901', 'Yet Another Book', 'User789', 5, 'Outstanding', 'One of the best reads of the year for me.')
# """
# )

# spark.sql(
#     """
# INSERT INTO books (Title, authors, categories, ratingsCount)
# VALUES ('New Book Title', 'Author Name', '["Fiction", "Adventure"]', 100)
# """
# )


# spark.sql(
#     """
# INSERT INTO books (Title, authors, description, publishedDate, publisher)
# VALUES ('Book by New Author', 'New Author', 'An insightful exploration...', '2023-12-01', 'New Publisher')
# """
# )


# spark.sql(
#     """
# INSERT INTO reviews (Id, Title, User_id, `review/helpfulness`, `review/score`)
# VALUES ('456789012', 'Book Title', 'User321', '15/16', 5)
# """
# )


# spark.sql(
#     """
# INSERT INTO books (Title, authors, description, image, previewLink, publisher, publishedDate, categories)
# VALUES ('Complete Book Title', 'Complete Author', 'A complete description of the book.', 'http://image.url', 'http://preview.url', 'Complete Publisher', '2024-01-01', '["Complete Category"]')
# """
# )

# spark.sql(
#     """
# INSERT INTO reviews (Id, Title, User_id, `review/score`, `review/summary`, `review/text`)
# VALUES
# ('567890123', 'Innovative Read', 'User980', 5, 'Innovation at its Best', 'The innovative narrative structure of this book sets a new benchmark in literature.')
# """
# )

# spark.sql(
#     """
# INSERT INTO books (Title, authors, description, publishedDate)
# VALUES
# ('Future of Fiction', 'Visionary Author', 'A book that redefines the boundaries of fiction with its innovative storytelling.', '2024-05-01')
# """
# )

# spark.sql(
#     """
# INSERT INTO reviews (Id, Title, User_id, `review/score`, `review/summary`, `review/text`)
# VALUES
# ('678901234', 'Future of Fiction', 'User112', 4.5, 'Nearly Perfect', 'While the book reaches for the stars with its ambitious vision, it falls slightly short on character development. Nonetheless, it is a must-read for its groundbreaking narrative techniques.')
# """
# )

# start_time = time.time()

# result1 = spark.sql(
#     """
# SELECT b.Title, AVG(r.`review/score`) as average_score
# FROM books b
# JOIN reviews r ON b.Title = r.Title
# GROUP BY b.Title
# ORDER BY average_score DESC
# """
# )

# # result1.show()

# result2 = spark.sql(
#     """
# SELECT b.authors, AVG(r.`review/score`) as avg_score
# FROM books b
# JOIN reviews r ON b.Title = r.Title
# GROUP BY b.authors
# ORDER BY avg_score DESC
# """
# )

# # result2.show()

# result3 = spark.sql(
#     """
# SELECT YEAR(r.`review/time`) as review_year, COUNT(*) as reviews_count
# FROM reviews r
# GROUP BY review_year
# ORDER BY review_year DESC, reviews_count DESC
# """
# )

# # result3.show()

# result4 = spark.sql(
#     """
# SELECT b.Title, COUNT(r.Id) as total_reviews, AVG(r.`review/score`) as avg_score
# FROM books b
# JOIN reviews r ON b.Title = r.Title
# GROUP BY b.Title
# HAVING COUNT(r.Id) > 10
# ORDER BY total_reviews DESC, avg_score DESC
# """
# )

# # result4.show()

# result5 = spark.sql(
#     """
# SELECT b.Title, b.authors, r.`review/summary`, r.`review/text`
# FROM books b
# CROSS JOIN reviews r
# WHERE b.authors LIKE '%Tolkien%'
# LIMIT 100
# """
# )

# result5.show()

# end_time = time.time()

# # result.show()
# print(f"Result completed in {end_time - start_time} seconds.")

# Stop the Spark Session
# wait = input("Press Enter to continue.")
spark.stop()

exit()


# Define a function to log performance
def log_performance(start_time, operation):
    end_time = time.time()
    print(f"{operation} completed in {end_time - start_time} seconds.")


df.show()

query = "(SELECT * FROM reviews WHERE `review/score` = 5 LIMIT 10; )"


# Perform 10 select operations
for i in range(10):
    start_time = time.time()
    df.select("*").limit(10).collect()
    log_performance(start_time, f"Select operation {i+1}")

# # Perform 10 self-join operations
# for i in range(10):
#     start_time = time.time()
#     # Assuming 'id' is a column in your DataFrame
#     df.alias("df1").join(df.alias("df2"), df["df1"]["id"] == df["df2"]["id"]).collect()
#     log_performance(start_time, f"Self-join operation {i+1}")

# Stop the Spark Session
spark.stop()
