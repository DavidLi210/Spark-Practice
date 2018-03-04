from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    conf = SparkConf().setAppName("Q2").setMaster("local")
    sc = SparkContext(conf=conf)
    data = sc.textFile("ratings.dat")
    ratings = data.map(lambda x: x.split('::')).map(lambda x: Rating(int(x[0]), int(x[1]), int(x[2])))
    # devide the data into test_data and train_data
    splits = ratings.randomSplit([6, 4], 24)
    rank = 10
    numIterations = 20
    train_data = splits[0]

    test_data = splits[1]
    # use train data to train model
    model = ALS.train(train_data, rank, numIterations)

    test_label = test_data.map(lambda p: ((p[0], p[1]), p[2]))
    test_data = test_data.map(lambda p: (p[0], p[1]))

    # use (user_id, movie_id) pair to get the test ratings
    predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))
    # join test_data and prediction to calculate mse between real ratings and estimated ratings
    result = predictions.join(test_label)
    mse = result.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = " + str(mse))

