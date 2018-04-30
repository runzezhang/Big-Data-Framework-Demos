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
    return pair[0],pair[1], commonList

if __name__=="__main__":
    #cite: http://spark.apache.org/docs/latest/sql-programming-guide.html#tab_python_0
    spark = SparkSession \
        .builder \
        .appName("sql_common_friends") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # Load a text file and convert each line to a Row.
    sc = spark.sparkContext

    # read files
    inputRDD = sc.textFile("soc-LiveJournal1Adj.txt", 1)
    # split id and his/her friend list
    FriendList_rdd = inputRDD.map(lambda x: re.split(r"\s+", x)) \
        .filter(lambda x: len(x[1]) != 0 and len(x) - 1 > 0)
    # print(processing_rdd.take(5))
    # split friend list as single value
    SingleList_rdd = FriendList_rdd.map(lambda x: (x[0], re.split(r"\D+", x[1])))
    # print(SingleList_rdd.take(5))
    # pair
    pair_rdd = SingleList_rdd.flatMap(pair)
    # print(pair_rdd.take(5))
    group_rdd = pair_rdd.groupByKey()
    # print(group_rdd.take(5))
    result_rdd = group_rdd.map(number).filter(lambda x: len(x[2]) > 0)

    schema_common = spark.createDataFrame(result_rdd) \
                         .distinct()
    count_udf = udf(lambda x: len(x), IntegerType())
    final_table = schema_common.select("_1","_2", count_udf("_3")).sort(["_3"], ascending=False)\
                               .repartition(1).write.save('./sql_common_friends', 'csv', 'overwrite')
