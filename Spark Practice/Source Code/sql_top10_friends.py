from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import udf, desc
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf
from pyspark.sql import *
from pyspark.sql import Row
from pyspark.sql.types import *
import re

def pair(friend_list):
    pair_list = []
    ID=friend_list[0]
    for friendx in friend_list[1]:
        if ID<friendx:
            pair_list.append(((ID,friendx),set(friend_list[1])))
        else:
            pair_list.append(((friendx,ID),set(friend_list[1])))
    return pair_list


def number(x):
    temp = set()
    pair = x[0]
    commonList = []
    for friendlist in x[1]:
        if len(temp) == 0:
            for friendx in friendlist:
                temp.add(friendx)
        else:
            for friendx in friendlist:
                if friendx in temp:
                    commonList.append(friendx)
    return pair, commonList


if __name__=="__main__":
    #cite: http://spark.apache.org/docs/latest/sql-programming-guide.html#tab_python_0
    spark = SparkSession \
        .builder \
        .appName("sql_top10_friends") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # Load a text file and convert each line to a Row.
    sc = spark.sparkContext

    # read files
    inputRDD = sc.textFile("soc-LiveJournal1Adj.txt", 1)
    inputRDD2 = sc.textFile("userdata.txt", 1)
    # split id and his/her friend list
    FriendList_rdd = inputRDD.map(lambda x: re.split(r"\s+", x)) \
        .filter(lambda x: (len(x[1])+1) != 1 and len(x) - 1 > 0)
    # print(processing_rdd.take(5))
    # split friend list as single value
    SingleList_rdd = FriendList_rdd.map(lambda x: (x[0], re.split(r"\D+", x[1])))
    # print(SingleList_rdd.take(5))
    # pair
    pair_rdd = SingleList_rdd.flatMap(pair)


    # print(pair_rdd.take(5))
    group_rdd = pair_rdd.groupByKey()

    # print(group_rdd.take(5))
    result_rdd = group_rdd.map(number).filter(lambda x: len(x[1]) > 0)


    count_udf = udf(lambda x: len(x), IntegerType())

    #sort and leave top 10
    schema_common = spark.createDataFrame(result_rdd).select("_1", count_udf("_2").alias("_2")).sort("_2", ascending=False).head(10)


    Top10 = sc.parallelize(schema_common).map(lambda x: Row(user_id=x[0][0], other=x[0][1]))
    top10_schema = spark.createDataFrame(Top10)

    detail = inputRDD2.map(lambda x: re.split(r",", x)).map(lambda x: Row(address=x[3], lastname=x[2],firstname=x[1], userid=x[0]))
    user_detail = spark.createDataFrame(detail)


    join1 = top10_schema.join(user_detail, top10_schema.user_id == user_detail.userid, "cross")\
        .selectExpr("userid as user_id", "firstname as fname", "lastname as lname", "address as addr", "other as other")


    join2 = join1.join(user_detail, join1.other == user_detail.userid, "cross").drop("userid"). \
        selectExpr("user_id as user_id", "fname as fname", "lname as lname", "addr as addr", "other as other",\
                   "firstname as firstname", "lastname as lastname", "address as address")

    join2.repartition(1).write.save('./sql_top10_friends', 'csv', 'overwrite')