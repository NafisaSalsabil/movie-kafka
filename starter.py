
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, from_unixtime, col
import os

from quality import BAD_DIR, alert_if_needed, check_ratings_spark, stage

# PART 1: STORE AND BATCH 
spark = SparkSession.builder.appName("Movie and Rating Analysis").master("local[*]").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR") 

# read csv files
movies = spark.read.csv("movies.csv", header = True, inferSchema = True) 
ratings = spark.read.csv("ratings.csv", header = True, inferSchema = True) 

# Count rows processed in Part 1
movie_count = movies.count()
ratings_count = ratings.count()

total_rows = movie_count + ratings_count

print("\n--- PART 1 ROW COUNT CHECK ---")
print(f"Movies rows: {movie_count:,}")
print(f"Ratings rows: {ratings_count:,}")
print(f"Total rows processed: {total_rows:,}")

if total_rows >= 1_000_000:
    print("Part 1 processes at least 1,000,000 rows.")

# finding csv file sizes 
movie_size = os.path.getsize("movies.csv") / (1024 * 1024)
ratings_size = os.path.getsize("ratings.csv") / (1024 * 1024) 

# print original csv sizes
print(f"movie.csv file size: {movie_size} MB")
print(f"ratings.csv file size: {ratings_size} MB")

# PART 4: quality on ratings before Parquet write
print("\n--- PART 4 QUALITY (batch) ---")
good, bad, fails = check_ratings_spark(ratings)
bad_n = bad.count()
good_n = good.count()
stage("batch-csv", ratings_count, good_n, bad_n)
alert_if_needed(bad_n, ratings_count, "batch-csv")
if bad_n > 0:
    bad.write.mode("overwrite").parquet(BAD_DIR)
    print(f"[QUALITY] quarantined {bad_n:,} rows -> {BAD_DIR}/", flush=True)
ratings = good

# convert unix timestamp to year 
ratings = ratings.withColumn("year", year(from_unixtime(col("timestamp"))))

# convert csv to parquet 
movies.write.mode("overwrite").parquet("movies.parquet")  
ratings.write.mode("overwrite").partitionBy("year").parquet("ratings.parquet")

# Verify Parquet row counts
movies_parquet_count = spark.read.parquet("movies.parquet").count()
ratings_parquet_count = spark.read.parquet("ratings.parquet").count()

print("\n--- PARQUET ROW COUNT CHECK ---")
print(f"Total Parquet rows: {movies_parquet_count + ratings_parquet_count:,}")
stage("batch-parquet", good_n, ratings_parquet_count, 0)

# calculate parquet file size
def parquet_size(path):
    size = 0
    for root, _, files in os.walk(path):
        for file in files:
            path = os.path.join(root, file)
            size += os.path.getsize(path)
    return size / (1024 * 1024)

# print parquet sizes 
print(f"movies_parquet file size: {parquet_size('movies.parquet')} MB")
print(f"ratings_parquet file size: {parquet_size('ratings.parquet')} MB")

# read parquet files 
movies = spark.read.parquet("movies.parquet")
ratings = spark.read.parquet("ratings.parquet")

movies.createOrReplaceTempView("movies")
ratings.createOrReplaceTempView("ratings")

# question #1 
top_movies = spark.sql("""
    SELECT m.movieId, m.title, AVG(r.rating) AS avg_rating, COUNT(r.rating) AS rating_count
    FROM ratings r
    JOIN movies m ON r.movieId = m.movieId
    GROUP BY m.movieId, m.title
    HAVING COUNT(r.rating) >= 1000
    ORDER BY avg_rating DESC
    LIMIT 10
""")

print("ten highest-rated movies with at least 1,000 ratings:")
top_movies.show(truncate=False)

# question #2 
top_genres = spark.sql("""
    SELECT year, genre, rating_count
    FROM (
        SELECT r.year, genre, COUNT(*) AS rating_count,
            RANK() OVER (
                PARTITION BY r.year
                ORDER BY COUNT(*) DESC
            ) AS genre_rank
        FROM ratings r
        JOIN movies m ON r.movieId = m.movieId
        LATERAL VIEW EXPLODE(SPLIT(m.genres, '\\\\|')) g AS genre
        GROUP BY r.year, genre
    ) ranked
    WHERE genre_rank = 1
    ORDER BY year
""")

print("genre received the most ratings in each year:")
top_genres.show(31, truncate=False)  

spark.stop()


# PART 2: Stream  -> producer.py / consumer.py


# PART 3: Serve


# PART 4: Quality and Monitoring  -> quality.py (wired into batch/producer/consumer)
