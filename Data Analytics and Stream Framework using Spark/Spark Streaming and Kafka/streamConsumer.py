import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from textblob import TextBlob
from io import StringIO

def get_tweet_sentiment(val):
    if float(val) > 0.05:
        return "positive"
    elif float(val) <0.05:
        return "negative"
    else:
        return "neutral"

sc = SparkContext(appName="PythonSparkStreamingKafka")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 60)

consumer = KafkaConsumer('twitterstream',bootstrap_servers=['localhost:9092'])
for message in consumer:
    str = message.value.decode("utf-8")
    tblob = TextBlob(str)
    for sen in tblob.sentences:
        print(sen)
        print(get_tweet_sentiment(sen.sentiment.polarity))
        print("\n")


ssc.start()
ssc.awaitTermination()
