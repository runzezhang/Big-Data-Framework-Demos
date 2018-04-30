from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

conf = SparkConf().setMaster("local").setAppName("Problem1")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()

# Load and parse the data
data = sc.textFile("ratings.dat").map(lambda line: line.split("::")).\
    map(lambda x: Rating(int(x[0]), int(x[1]), int(x[2])))
splits = data.randomSplit([6, 4], 24)
train_data = splits[0]
test_data = splits[1]
rank = 10
Iterations = 15
model = ALS.train(train_data, rank, Iterations)

test_label = test_data.map(lambda p: ((p[0], p[1]), p[2]))
test_data = test_data.map(lambda p: (p[0], p[1]))
predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))

combined_result = predictions.join(test_label)
MSE = combined_result.map(lambda r: (r[1][0] - r[1][1])**2).mean()
print("mean squared error = " + str(MSE))