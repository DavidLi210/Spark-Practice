import os
from kafka import KafkaConsumer
from textblob import TextBlob
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'


def analize_sentiments(val):
    '''
    analise attitude of sentence and return it
    :param val: a line in textblob
    :return: attitude string
    '''
    if float(val) > 0.35:
        return "positive"
    elif float(val) < -0.35:
        return "negative"
    else:
        return "neutral"


if __name__ == "__main__":
    # create kafka consumer
    consumer = KafkaConsumer('my-topic',
                             group_id='my-group',
                             bootstrap_servers=['localhost:9092'])
    # iterate and decode message in consumer, then use textblob to analyse it attitude
    for msg in consumer:
        message = msg.value.decode("utf-8")
        tblob = TextBlob(message)
        for sen in tblob.sentences:
            print sen
            print analize_sentiments(sen.polarity)
            print ""

