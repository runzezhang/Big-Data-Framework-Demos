1.Clustering
Using spark machine learning library spark-mlib, use kmeans to cluster the movies using the ratings given by the user, that is, use the item-user matrix from itemusermat File provided as input to your program.
2.Use Collaborative  ltering  nd the accuracy(MSE) of ALS model accuracy.
3.Advanced Analytics (Classi cation using Kernel Mean Matching)
Use KMM algorithm to do a bias correction on the app dataset. Then use this bias corrected data and do a classi cation to predict the app using any state-of-the-art machine learning classi cation algorithm. Measure the accuracy of the classi cation
 
on the bias corrected data. Also, apply that classi cation algorithm on biased dataset (without using KMM) and measure the accuracy. Compare two accuracies.
Source code of KMM is provided in the links section. You may use any machine learning packages or libraries (e.g. Scikit Learn, TensorFlow etc.)
4. Spark Streaming and Kafka
1. Scrapper (for python, but scala needs to produce same result)
The scrapper will collect all tweets and sends them to Kafka for analytics. The scraper will be a standalone program written in JAVA/PYTHON and should perform the followings:
a. Collecting tweets in real-time with particular hash tags. For example, we will collect all tweets with #trump, #obama.
b. After getting tweets we will  lter them based on their locations. If any tweet does not have location, we will discard them.
c. After  ltering, we will send them (tweets with location) to Kafka in case if you use Python.
d. You should use Kafka API (producer) in your program (https://kafka.apache.org/090/documentation.html#producerapi)
e. You scrapper program will run in nitely and should take hash tag as input parameter while running.
2. Kafka (for Python)
     
◦ You need to install Kafka and run Kafka Server with Zookeeper. You should create a dedicated channel/topic for data transport
3. Sentiment Analyzer
◦ Sentiment Analysis is the process of determining whether a piece of writing is positive, negative or neutral. It's also known as opinion mining, deriving the opinion or attitude of a speaker.
For example,
“President Donald Trump approaches his  rst big test this week from a position of unusual weakness.” - has positive sentiment.
“Trump has the lowest standing in public opinion of any new president in modern history.” - has neutral sentiment.
“Trump has displayed little interest in the policy itself, casting it as a thankless chore to be done before getting to tax-cut legislation he values more.” - has negative sentiment.
The above examples are taken from CNBC news:
http://www.cnbc.com/2017/03/22/trumps- rst-big-test-comes-as-hes-in-an- unusual-position-of-weakness.html
You can use any third party sentiment analyzer like Stanford CoreNLP (java/scala), nltk(python) for sentiment analyzing. For example, you can add Stanford CoreNLP as an external library using SBT/Maven in your scala/java project. In python you can import nltk by installing it using pip.
Sentiment analysis using Spark Streaming:
In Spark Streaming, create a Kafka consumer (for python, shown in the class for streaming) and periodically collect  ltered tweets (required for both scala and python) from scrapper. For each hash tag, perform sentiment analysis using Sentiment Analyzing tool (discussed above). Then for each hash tag, save the output with twitter itself.
