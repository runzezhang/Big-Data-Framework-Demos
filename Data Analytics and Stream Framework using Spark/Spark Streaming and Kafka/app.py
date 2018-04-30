import json
from kafka import SimpleProducer, KafkaClient
import tweepy
import configparser

mytopic = 'twitterstream'
class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twitter stream and push it to Kafka"""

    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client, async = True,
                          batch_send_every_n = 1000,
                          batch_send_every_t = 10)

    def on_status(self, status):
        msg =  status.text.encode('utf-8')
        try:
            self.producer.send_messages(mytopic, msg)
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):

        print('Got an error with status code: ' + str(status_code))
        return True  # To continue listening

    def on_timeout(self):
        return True

if __name__ == '__main__':
    #https://apps.twitter.com/app/14544294/keys
    # twitter name runzezhang
    # tweet connections
    access_token = "714107015358849026-Uz5K8rxCd9UrtAZQHsT80DfyxDK1ytx"
    access_token_secret = "HPbE2f1Hdv6dbccCQSh9qcDAkEL008mYLLv7l1x922iit"
    consumer_key = "VsXKiECNEKmR2nltWoMrigsFN"
    consumer_secret = "tG4HpTDNxKjwh3KkLVMlwNhbc6gDXecDOf3QHmkxFcTrsB6koU"

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))

    stream.filter(track=['trump', 'obama'], languages = ['en'])
