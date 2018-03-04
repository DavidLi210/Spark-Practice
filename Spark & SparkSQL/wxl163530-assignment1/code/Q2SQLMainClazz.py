from pyspark import SparkContext, SparkConf, Row
import re
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def step1(infos):
    '''
    :param infos: generate all possible common friends pair
    :return: pairs of friends
    '''
    me = infos[0]
    res = [((me, other), infos[1]) if me < other else ((other, me), infos[1]) for other in infos[1]]
    return res


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
    return pair, res


def step3(x):
    '''
    :param x: a line in the file
    :return: uid,( firstname, lastname, address)
    '''
    infos = re.split(r",", x)
    return infos[0], (infos[1], infos[2], infos[3])


def step4(x):
    return x[0][0], x


if "__main__" == __name__:
    spark = SparkSession.builder.appName("Q1SQL"). \
        config("spark.some.config.option", "some-value"). \
        getOrCreate()
    sc = spark.sparkContext

    # generate all possible common friends pair
    rdd1 = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: re.split(r"\s+", x))\
                                                            .filter(lambda x: len(x) > 1 and len(x[1]) != 0)\
                                                            .map(lambda x: (x[0], re.split(r"\D+", x[1])))\
                                                            .flatMap(step1)
    # group values by common key
    rdd3 = rdd1.groupByKey()
    # get top10 common friend pair that has the most number of common friends
    rdd4 = rdd3.map(step2).filter(lambda x: len(x[1]) > 0)

    # use customized function to get len of common friends
    slen = udf(lambda x: len(x), IntegerType())
    # sort key value pairs by number of common friends and get the top10 as list
    topList = spark.createDataFrame(rdd4).select("_1", slen("_2").alias("_2")).sort("_2", ascending=False).head(10)

    # convert top10 list into df
    tempTop10 = sc.parallelize(topList).map(lambda x: Row(uid=x[0][0], other=x[0][1]))
    top10 = spark.createDataFrame(tempTop10)

    userdata = sc.textFile("userdata.txt")
    userdata2 = userdata.map(lambda x: re.split(r",", x)).map(lambda x: Row(address=x[3], lastname=x[2],\
                                                                            firstname=x[1], uid2=x[0]))
    userinfo = spark.createDataFrame(userdata2)

    # join userinfo and left friend id
    res0 = top10.join(userinfo, top10.uid == userinfo.uid2, "cross")\
        .selectExpr("uid2 as uid", "firstname as fname", "lastname as lname", "address as addr", "other as other")

    # join userinfo and right friend id
    res = res0.join(userinfo, res0.other == userinfo.uid2, "cross").drop("uid2"). \
        selectExpr("uid as uid", "fname as fname", "lname as lname", "addr as addr", "other as other",\
                   "firstname as firstname", "lastname as lastname", "address as address")
    res.printSchema()
    print(res.count())
    res.repartition(1).write.save('./result1', 'csv', 'overwrite')