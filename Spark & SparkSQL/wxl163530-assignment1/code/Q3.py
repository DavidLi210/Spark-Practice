from pyspark import SparkContext, SparkConf
import re


def businesssplit(x):
    infos = re.split("::", x)
    return infos[0],(infos[1], infos[2])


if "__main__" == __name__:
    conf = SparkConf().setAppName("Q3").setMaster('local')
    sc = SparkContext(conf=conf)
    review = sc.textFile("review.csv")
    business = sc.textFile("business.csv")
    rdd1 = review.map(lambda x: (re.split("::", x)[2], re.split("::", x)[1]))
    # caculate number of users for each business and sort it in ascending order from review table
    rdd2 = rdd1.distinct().map(lambda x: (x[0], 1))\
        .reduceByKey(lambda x, y: x + y)\
        .sortBy(lambda x: x[1], ascending=False, numPartitions=1)

    # take the top10 business that has the most number of users, and join it with business
    top10 = business.map(businesssplit).join(rdd2).distinct().top(10, key=lambda t: t[1][1])
    rdd4 = sc.parallelize(top10)
    rdd4.saveAsTextFile("./result6")
