from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.ml.clustering import KMeans
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors


conf = SparkConf().setMaster("local").setAppName("Question1")
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .getOrCreate()

item_user_mat = sc.textFile("itemusermat").map(lambda x: x.split(" ")).map(lambda x: [int(y) for y in x])

# Create an RDD of indexed rows.
data = [(Vectors.dense(x),) for x in item_user_mat.collect()]
item_user_mat = spark.createDataFrame(data, ["features"])

# train k-means clustering algorithm
kmeans = KMeans(k=10, seed=1)
model = kmeans.fit(item_user_mat)

# predict
transformed = model.transform(item_user_mat).select("features", "prediction")
transformed_with_index = transformed.rdd.zipWithIndex()
rows = transformed_with_index.collect()
prediction_with_index = sc.parallelize(rows).map(lambda x: (x[1], x[0].prediction))

# output result
movie = sc.textFile('movies.dat').map(lambda x: x.split("::")).map(lambda idx_title_types: (int(idx_title_types[0]), (idx_title_types[1], idx_title_types[2])))

tmp = movie.join(prediction_with_index).map(lambda x_y: (x_y[1][1], (x_y[1][1], x_y[0], x_y[1][0][0], x_y[1][0][1])))
result = tmp.groupByKey().map(lambda x: list(x[1])[0:5]).flatMap(lambda x: x).map(lambda x:'cluster_id:{}, movie_id:{}, movie:"{}", genre:"{}"'.format(*x)).collect()
sc.parallelize(result).saveAsTextFile("question1")

