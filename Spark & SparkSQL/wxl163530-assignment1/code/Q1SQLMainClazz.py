from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType


def step1(infos):
    me = infos[0]
    return [((me, other), infos[1]) if me < other \
                else ((other, me), infos[1]) \
            for other in infos[1]]


def step2(tple):
    '''
    :param tple: tple[0] is pair as key, tple[1] is a list which contains each line's friends as list
    :return:
    '''
    pair = tple[0]
    set1 = set({})
    res = []
    for friends in tple[1]:
        if len(set1) == 0:
            for c in friends:
                set1.add(c)
        else:
            for c in friends:
                if c in set1:
                    res.append(c)
    return pair[0], pair[1], res


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Q1SQL"). \
        config("spark.some.config.option", "some-value").\
        getOrCreate()

    sc = spark.sparkContext
    # generate all possible common friends pair
    rdd1 = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: re.split(r"\s+", x)) \
        .filter(lambda x: len(x) > 1 and len(x[1]) != 0) \
        .map(lambda x: (x[0], re.split(r"\D+", x[1]))) \
        .flatMap(step1)

    # group values by common key
    rdd3 = rdd1.groupByKey()
    rdd4 = rdd3.map(step2).filter(lambda x: len(x[2]) > 0)
    # sort key value pairs by number of common friends
    df = spark.createDataFrame(rdd4)
    df.printSchema()

    # use customized function to get len of common friends
    slen = udf(lambda x: len(x), IntegerType())
    df2 = df.select("_1","_2", slen("_3")).sort(["_1", "_2"], ascending=[0, 1]).\
        repartition(1).write.save('./result1', 'csv', 'overwrite')

    # df2 = df.withColumn("_2", len(df._2)).printSchema()
    # sort("_1", ascending=True)
    # df.rdd.repartition(1).saveAsTextFile("./result1")
