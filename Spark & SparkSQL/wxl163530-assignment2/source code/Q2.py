from pyspark import SparkContext,SparkConf
import re


def lineToPair(line):
    """
    generate key value pair as uid, friend id
    :param line: each line in input
    :return: list [uid, friends_id...]
    """
    friends = re.split(r",", line[1])
    friends = map(int, friends)

    return [int(line[0]), list(friends)]


def pairToMultiplePair(pair):
    """
    generate (uid, freind id) friends for each friend_id, always place smaller id at first position
    :param pair: [int(line[0]), list(friends)]
    :return: (uid, freind id),friends
    """
    l = []
    for i in range(len(pair[1])):
        if pair[0] < pair[1][i]:
            l.append(((pair[0], pair[1][i]), pair[1]))
        else:
            l.append(((pair[1][i], pair[0]), pair[1]))
    return l


def pairsToCommonFriends(pairs):
    """
    get common friends between two people
    :param pairs: pairs[0] is (uid1, uid2) pairs[1] contains their friends respectively
    :return: uid1, uid2's common friends
    """
    key = pairs[0]
    value = set(pairs[1][0]).intersection(set(pairs[1][1]))
    return key, value


if __name__ == '__main__':
    conf = SparkConf().setAppName("Q2").setMaster('local')
    sc = SparkContext(conf=conf)
    # generate (uid, freind id),friends
    data_file = sc.textFile("soc-LiveJournal1Adj.txt")\
        .map(lambda x:re.split(r"\t+", x)).filter(lambda x: len(x[1]) > 0).map(lineToPair)\
        .flatMap(pairToMultiplePair)
    # reduce (uid, freind id) by key and sort by number of common friends
    joined_data = data_file.reduceByKey(lambda x, y: [x, y]).map(pairsToCommonFriends).sortBy(lambda x: x[1], ascending=False)
    top10 = joined_data.keys().take(10)
    top10rdd = sc.parallelize(top10)
    user_info_file = sc.textFile("userdata.txt").map(lambda x: re.split(r",", x)).map(lambda x: (int(x[0]), x[1:3]))
    # take top10 and join them with movie info table twice to get movie details
    result_rdd = top10rdd.leftOuterJoin(user_info_file).map(lambda x: (x[1][0], [x[0]] + list(x[1][1:])))

    result_rdd = result_rdd.leftOuterJoin(user_info_file).map(lambda x: (x[1][0][0], x[1][0][1][0], x[1][0][1][1],\
                                                                         x[0], x[1][1][0], x[1][1][1]))
    print(result_rdd.top(1))
    result_rdd.saveAsTextFile("q2_result")