from pyspark import Row
import re

from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Q3") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext
    lines = sc.textFile("ratings.csv").map(lambda x: re.split(r",", x)).filter(lambda x: x[0].isdigit())
    # userId, movieId, rating, timestamp
    # create schema
    col_names = lines.map(lambda x: Row(userId=x[0], movieId=x[1], rating=float(x[2]), timestamp=x[3]))
    ratings_df = spark.createDataFrame(col_names)
    # group by movie id and caculate avg ratings
    avg_rating_df = ratings_df.select(ratings_df.movieId, ratings_df.rating).groupBy('movieId').avg('rating')
    # avg_rating_df.repartition(1).write.save('./q3_result', 'csv', 'overwrite')

    avg_rating_df.printSchema()
    # get top 10 movie with highest avg ratings
    avg_rating_df.sort("avg(rating)", ascending=True).show(10)

    # userId, movieId, tag, timestamp
    lines = sc.textFile("tags.csv").map(lambda x: re.split(r",", x)).filter(lambda x: x[0].isdigit())
    col_names = lines.map(lambda x: Row(userId=x[0], movieId=x[1], tag=x[2], timestamp=x[3]))
    tags_df = spark.createDataFrame(col_names)
    tags_df = tags_df.filter(tags_df["tag"] == "action")
    print(tags_df.take(5))
    # join with tag table and select movie with action tag
    joined_tags_df = avg_rating_df.join(tags_df, tags_df.movieId == avg_rating_df.movieId, 'inner').\
        select(avg_rating_df.movieId, avg_rating_df["avg(rating)"], tags_df.tag)
    joined_tags_df.show()

    # movieId, title, genres
    # join movie with action tag with movies table to select movie with THRILL as genres
    lines = sc.textFile("movies.csv").map(lambda x: re.split(r",", x)).filter(lambda x: x[0].isdigit())
    col_names = lines.map(lambda x: Row(movieId=x[0], title=x[1], genres=x[2]))
    movies_df = spark.createDataFrame(col_names)
    movies_df = movies_df.filter(movies_df["genres"].like('%Thriller%'))

    movies_tags_ratings_df = joined_tags_df.join(movies_df, movies_df.movieId == joined_tags_df.movieId, 'inner').\
        select(joined_tags_df.movieId, joined_tags_df["avg(rating)"], joined_tags_df.tag, movies_df.genres)
    movies_tags_ratings_df.show()
