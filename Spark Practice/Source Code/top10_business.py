from pyspark import SparkContext, SparkConf
import re

if __name__=="__main__":
    #create a SparkContext object
    conf = SparkConf().setAppName("top10_business").setMaster("local")
    sc = SparkContext(conf=conf)
    #read files
    business_rdd = sc.textFile("business.csv")
    review_rdd = sc.textFile("review.csv")
    #map:business_id and user_id
    reviewMap_rdd=review_rdd.map(lambda x:(re.split("::", x)[2], re.split("::", x)[1]))
    #delete one user multiple rate in same business
    distinct_rdd=reviewMap_rdd.distinct()
    #caculate rate number of business
    #???????reduceByKey
    number_rdd=distinct_rdd.map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)\
                           .sortBy(lambda x: x[1], ascending=False, numPartitions=1)
    businessMap_rdd=business_rdd.map(lambda x:(re.split("::", x)[0], (re.split("::", x)[1],re.split("::",x)[2])))
    #('piXuRfZ81xFGA64WFJrKkQ', (('926 Broxton AveWestwoodLos Angeles, CA 90024', 'List(Food, Desserts, Bakeries, Ice Cream & Frozen Yogurt)'), 2634)
    join_rdd=businessMap_rdd.join(number_rdd).distinct().top(10, key=lambda t: t[1][1])
    #print(join_rdd)
    print=sc.parallelize(join_rdd).saveAsTextFile("./top10_business")








