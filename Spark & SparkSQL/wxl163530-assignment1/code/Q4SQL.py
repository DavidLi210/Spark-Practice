from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    session = SparkSession.builder.appName("Q4SQL").\
        config("spark.some.config.option", "some-value").\
        getOrCreate()
    sc = session.sparkContext
    review = sc.textFile("review.csv").map(lambda x: x.split("::"))
    business = sc.textFile("business.csv").map(lambda x: x.split("::"))

    # create user defined schema
    schemaString1 = "rid uid bid stars"
    schemaString2 = "bid address category"
    rfields = [StructField(field_name, StringType(), True) for field_name in schemaString1.split()]
    bfields = [StructField(fname, StringType(), True) for fname in schemaString2.split()]
    rschema = StructType(rfields)
    bschema = StructType(bfields)

    # transfer rdd into df using my schema
    rs = session.createDataFrame(review, rschema)
    bs = session.createDataFrame(business, bschema)
    rs.printSchema()
    bs.printSchema()
    # filter out columns that do not contain Palo Alto
    df1 = rs.select(rs.bid, rs.uid, rs.stars)
    df = bs.filter(bs.address.like('%Palo Alto%'))

    # join review and business table
    df2 = df.join(df1, df.bid == df1.bid, 'inner')
    df2.select('uid', df2.stars.cast('int').alias('stars')).groupBy('uid').avg("stars").repartition(1).write.\
        save('./result', 'csv', 'overwrite')

