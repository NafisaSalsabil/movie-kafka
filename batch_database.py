"""Load batch aggregates from existing Parquet without rewriting stored data."""
from database import load_batch

def load_stored_results(spark, movies=None, ratings=None):
    movies = movies if movies is not None else spark.read.parquet("movies.parquet")
    ratings = ratings if ratings is not None else spark.read.parquet("ratings.parquet")
    movies.createOrReplaceTempView("movies")
    ratings.createOrReplaceTempView("ratings")
    movie_stats = spark.sql("""
        SELECT movieId, AVG(rating) avg_rating, COUNT(*) rating_count
        FROM ratings GROUP BY movieId
    """)
    genre_counts = spark.sql("""
        SELECT r.year, genre, COUNT(*) rating_count
        FROM ratings r JOIN movies m ON r.movieId=m.movieId
        LATERAL VIEW EXPLODE(SPLIT(m.genres, '\\\\|')) g AS genre
        GROUP BY r.year, genre
    """)
    load_batch(
        ((r.movieId, r.title) for r in movies.toLocalIterator()),
        ((r.movieId, r.avg_rating, r.rating_count) for r in movie_stats.toLocalIterator()),
        ((r.year, r.genre, r.rating_count) for r in genre_counts.toLocalIterator()),
    )

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("LoadBatchDatabase")
             .config("spark.sql.shuffle.partitions","4")
             .config("spark.sql.session.timeZone","UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    load_stored_results(spark)
    spark.stop()
