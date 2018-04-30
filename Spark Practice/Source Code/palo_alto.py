from pyspark import SparkContext, SparkConf
import re

if __name__=="__main__":
    #create a SparkContext object
    conf = SparkConf().setAppName("palo_alto").setMaster("local")
    sc = SparkContext(conf=conf)
    #read files
    business_rdd = sc.textFile("business.csv")
    review_rdd = sc.textFile("review.csv")

    businessMap_rdd=business_rdd.map(lambda x:(re.split("::", x)[0], (re.split("::", x)[1],re.split("::",x)[2])))
    filter_rdd=businessMap_rdd.filter(lambda x: "Palo Alto" in x[1][0])
    #print(filter_rdd.take(5))
    reviewMap_rdd = review_rdd.map(lambda x: (re.split("::", x)[2], (re.split("::", x)[1],re.split("::",x)[3])))
    #join and take user_id and rating
    join_rdd=reviewMap_rdd.join(filter_rdd).map(lambda x: (x[1][0][0], x[1][0][1]))\
    .mapValues(lambda x: (x, 1))\
    .reduceByKey(lambda x, y : (float(x[0]) + float(y[0]), (x[1] + y[1])))

    print(join_rdd.take(5))
    final = join_rdd.map(lambda x: (x[0], float(x[1][0]) / x[1][1]))
    #[('uXl4CdHvBDu2qVSGVhxtXw', '5.0')]
    print=final.repartition(1).saveAsTextFile("./palo_alto")