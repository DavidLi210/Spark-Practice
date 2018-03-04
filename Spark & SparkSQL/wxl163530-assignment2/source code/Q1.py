from pyspark import SparkContext,SparkConf
import re

given_sets = ((0, 4), (20, 22939), (1, 29826), (6222, 19272), (28041, 28056))

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


def addUpFriends(x, y):
    return [x, y]


def selectPairs(pair):
    """
    :param pair: common friends between each pair of users
    :return: true if (uid1, uid2) exists in given set
    """
    return pair[0] in given_sets


if __name__ == '__main__':
    conf = SparkConf().setAppName("Q1").setMaster('local')
    sc = SparkContext(conf=conf)
    # generate (uid, freind id),friends
    data_file = sc.textFile("soc-LiveJournal1Adj.txt")\
        .map(lambda x:re.split(r"\t+", x)).filter(lambda x: len(x[1]) > 0).map(lineToPair)\
        .flatMap(pairToMultiplePair)
    print(data_file.top(1))
    # reduce (uid, freind id) by key
    joined_data = data_file.reduceByKey(addUpFriends).map(pairsToCommonFriends).filter(selectPairs)
    joined_data.saveAsTextFile("q1_result")
