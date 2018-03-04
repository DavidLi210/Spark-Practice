from pyspark import SparkContext,SparkConf
import re


def generateBusinessPair(x):
    infos = re.split("::", x)
    return infos[0], (infos[1], infos[2])


def helper(x, y):
    return str(float(x[0]) + float(y[0])), x[1] + y[1]


if "__main__" == __name__:
    conf = SparkConf().setAppName("Q4").setMaster("local")
    sc = SparkContext(conf=conf)
    review = sc.textFile("review.csv")
    business = sc.textFile("business.csv")

    # filter out columns that do not contain Palo Alto
    rdd1 = business.map(generateBusinessPair).distinct().filter(lambda x: "Palo Alto" in x[1][0])
    rdd2 = review.map(lambda x: re.split("::", x)).map(lambda x: (x[2], (x[1], x[3])))
    # join review and business table
    rdd3 = rdd2.join(rdd1).map(lambda x: (x[1][0][0], x[1][0][1])).\
        mapValues(lambda x: (x, 1)).reduceByKey(helper)
    # caculate avg rate
    rdd4 = rdd3.map(lambda x: (x[0], float(x[1][0]) / x[1][1]))
    rdd4.repartition(1).saveAsTextFile("./result1")



