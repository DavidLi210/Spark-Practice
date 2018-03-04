import tweepy
from kafka import SimpleProducer, SimpleClient


class MyStreamListener(tweepy.StreamListener):

    def __init__(self, api=None):
        # create kafka producer and bind it to listener
        super(MyStreamListener, self).__init__(api)
        client = SimpleClient('localhost:9092')
        producer = SimpleProducer(client)
        self.kafProducer = producer

    def on_status(self, status):
        # encode msg and let kafka producer send it to kafka
        msg = status.text.encode("utf-8")
        try:
            self.kafProducer.send_messages("my-topic", msg)
        except Exception as e:
            print e

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            print "420 error"
            print status_code
            return False


if __name__ == "__main__":
    access_token = "803272565871480832-QszNghG7Nz6nYkkaxIbNCLLeD3vfKMi"
    access_token_secret = "W7olcLTguSTJOBNoG2f2IAl0lHxSzCrizWQU5yN9Cb2jY"
    consumer_key = "XZI31VB8cXF0wyaXyo9oLubdk"
    consumer_secret = "ywlkoZjLag0Ko5ReNTUUrds0x7ceGbL68CQgbHzb9ssiuogHAx"
    # establish twitter connections with token and secret
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    myStreamListener = MyStreamListener()
    # use listener to listen to the tweet stream and filter out unrelated tweets
    myStream = tweepy.Stream(auth = api.auth, listener = myStreamListener)
    myStream.filter(track=['#trump', '#obama'], languages=['en'], async=True)