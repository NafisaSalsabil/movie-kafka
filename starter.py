
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, from_unixtime, col
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
import os
import json

from batch_database import load_stored_results
from observability import record_quality, stage_event

from quality import BAD_DIR, alert_if_needed, check_ratings_spark, stage

# PART 1: STORE AND BATCH 
stage_event("batch", "running")
spark = SparkSession.builder.appName("Movie and Rating Analysis").master("local[2]").config("spark.sql.shuffle.partitions", "4").config("spark.driver.memory", "1g").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sparkContext.setLogLevel("ERROR") 

# read csv files
movies_schema = StructType([StructField("movieId", LongType()),
    StructField("title", StringType()), StructField("genres", StringType())])
ratings_schema = StructType([StructField("userId", LongType()),
    StructField("movieId", LongType()), StructField("rating", DoubleType()),
    StructField("timestamp", LongType())])
# Invalid numeric strings become null and are quarantined by the missing check.
movies = spark.read.schema(movies_schema).option("enforceSchema", "false").csv("movies.csv", header=True)
ratings = spark.read.schema(ratings_schema).option("enforceSchema", "false").csv("ratings.csv", header=True) 

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
good = good.persist()
bad_n = bad.count()
good_n = good.count()
stage("batch-csv", ratings_count, good_n, bad_n)
alert_if_needed(bad_n, ratings_count, "batch-csv")
if bad_n > 0:
    bad.write.mode("overwrite").parquet(BAD_DIR)
    print(f"[QUALITY] quarantined {bad_n:,} rows -> {BAD_DIR}/", flush=True)
record_quality("batch", 0, ratings_count, good_n, fails,
               [r.asDict() for r in bad.limit(20).collect()] if bad_n else [])
ratings = good
assert good_n + bad_n == ratings_count, "Batch quality row accounting failed"

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
assert ratings_parquet_count == good_n and movies_parquet_count == movie_count, "Parquet row conservation failed"

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
os.makedirs("reports", exist_ok=True)
with open("reports/storage_sizes.json", "w") as handle:
    json.dump({"movies": {"rows": movie_count, "csv_bytes": os.path.getsize("movies.csv"),
              "parquet_bytes": round(parquet_size("movies.parquet") * 1024 * 1024)},
              "ratings": {"rows_in": ratings_count, "rows_out": ratings_parquet_count,
              "csv_bytes": os.path.getsize("ratings.csv"),
              "parquet_bytes": round(parquet_size("ratings.parquet") * 1024 * 1024)}}, handle, indent=2)
stage("movies-parquet", movie_count, movies_parquet_count, 0)

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
    ORDER BY avg_rating DESC, m.movieId
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

# PART 3: load complete aggregates for database ranking queries.
load_stored_results(spark, movies, ratings)
stage_event("batch", "complete", {"ratings_in": ratings_count, "ratings_out": ratings_parquet_count,
                                   "movies": movies_parquet_count})

spark.stop()


# PART 2: Stream  -> producer.py / consumer.py


# PART 3: Serve -> database.py / queries.py / serve.py


# PART 4: Quality and Monitoring  -> quality.py (wired into batch/producer/consumer)
