from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql import Row

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q3SQL"). \
        config("spark.some.config.option", "some-value").\
        getOrCreate()

    sc = spark.sparkContext
    rrdd = sc.textFile("review.csv").map(lambda x: x.split("::"))
    brdd = sc.textFile("business.csv").map(lambda x: x.split("::"))
    # caculate number of users for each business and sort it in ascending order from review table
    temp = rrdd.map(lambda x: Row(bid=x[2], uid=x[1]))
    dfr = spark.createDataFrame(temp).distinct().groupBy('bid').count()
    # take the top10 business that has the most number of users
    top10 = dfr.sort("count", ascending=False).head(10)
    top10 = spark.createDataFrame(sc.parallelize(top10))
    top10.printSchema()
    print(top10.head(10))
    tempb = brdd.map(lambda x: Row(bid=x[0], address=x[1], category=x[2]))
    dfb1 = spark.createDataFrame(tempb)
    dfb1.printSchema()

    # join top10 business's id with business table
    dfb2 = top10.join(dfb1, top10.bid == dfb1.bid, 'inner').select(dfb1.bid, dfb1.address, dfb1.category, 'count').distinct()
    dfb2.repartition(1).write.save('./result', 'csv', 'overwrite')

