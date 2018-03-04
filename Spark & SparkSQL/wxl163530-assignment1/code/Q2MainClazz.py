
from pyspark import SparkContext,SparkConf
import re


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
    return pair, len(res)


def step3(x):
    '''
    :param x: a line in the file
    :return: firstname, lastname, address
    '''
    infos = re.split(r",", x)
    return infos[0], (infos[1], infos[2], infos[3])


def step4(x):
    return x[0][0], x


if "__main__" == __name__:
    conf = SparkConf().setAppName("Q1").setMaster('local')
    sc = SparkContext(conf=conf)
    # generate all possible common friends pair
    rdd1 = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: re.split(r"\s+", x))\
                                                            .filter(lambda x: len(x) > 1 and len(x[1]) != 0)\
                                                            .map(lambda x: (x[0], re.split(r"\D+", x[1])))\
                                                            .flatMap(step1)
    # group values by common key
    rdd3 = rdd1.groupByKey()
    # get top10 common friend pair that has the most number of common friends
    rdd4 = rdd3.map(step2).filter(lambda x: x[1] > 0).sortBy(lambda x: x[1], ascending=False).take(10)

    top10 = sc.parallelize(rdd4)
    userrdd = sc.textFile("userdata.txt").map(step3)

    # join user details info with top10 user by user id
    rdd5 = top10.keys().join(userrdd).map(lambda x: (x[1][0], (x[0], x[1][1])))\
                                                                    .join(userrdd)\
                                                                    .map(lambda x: (x[1][0], (x[0], x[1][1])))

    rdd5.repartition(1).saveAsTextFile("./result1")