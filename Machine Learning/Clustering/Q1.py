from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.clustering import KMeans


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1").config("spark.some.config.option", "some-value").getOrCreate()
    sc = spark.sparkContext
    rates = sc.textFile("itemusermat").map(lambda x: x.split(" ")).map(lambda x: [int(i) for i in x])\
        .collect()

    # make use of kmean model to fit the matrix
    rates = [(Vectors.dense(x),) for x in rates]
    df = spark.createDataFrame(rates, ["features"])
    kmeans = KMeans(k=10, seed=1)
    model = kmeans.fit(df)

    # group all movies by cluster
    transformed = model.transform(df).select("prediction", "features").rdd.map(lambda x: (int(x.prediction), int(list(x.features)[0]))). \
        groupByKey().mapValues(lambda x: list(x)[:5])

    # create pair in format of (movie_id, cluster_id)
    # generate pairs like (movie_id, cluster_id) from x which is in the form of (cluster_id, [id1,id2,id3...])
    fiveincluster = transformed.flatMap(lambda x : [(a, x[0]) for a in x[1]])

    # read movie info file
    moviedetails = sc.textFile("movies.dat").map(lambda x: x.split("::")).map(lambda x: (int(x[0]), (x[1], x[2])))

    # join the (movie_id, cluster_id) and movie info
    result = fiveincluster.join(moviedetails).map(lambda x: (x[1][0], x[0], x[1][1][0], x[1][1][1])).sortBy(lambda x: x[0])
    result.repartition(1).saveAsTextFile("./result")





