
from pyspark import SparkContext,SparkConf
import re


def step1(infos):
    '''
    :param infos: generate all possible common friends pair
    :return: pairs of friends
    '''
    me = infos[0]
    res = [((me, other), infos[1]) if me < other else ((other, me), infos[1]) for other in infos[1]]
    # f1 = infos[0]
    # for friend in infos[1]:
    #     f2 = friend
    #     if f1 > f2:
    #         temp = f1
    #         f1 = f2
    #         f2 = temp
    #     res.append(((f1, f2), infos[1]))
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
    rdd4 = rdd3.map(step2).filter(lambda x: x[1] > 0).sortBy(lambda x: x[1], ascending=False)
    rdd4.saveAsTextFile("./result1")