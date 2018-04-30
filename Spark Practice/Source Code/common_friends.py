from pyspark import SparkContext, SparkConf
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
    common = []
    for friendlist in x[1]:
        if len(temp) == 0:
            for friendx in friendlist:
                temp.add(friendx)
        else:
            for friendx in friendlist:
                if friendx in temp:
                    common.append(friendx)
    return pair, len(common)

if __name__=="__main__":
    #create a SparkContext object
    conf = SparkConf().setAppName("common_friends").setMaster("local")
    sc = SparkContext(conf=conf)
    #read files
    inputRDD = sc.textFile("soc-LiveJournal1Adj.txt",1)
    #split id and his/her friend list
    FriendList_rdd= inputRDD.map(lambda x: re.split(r"\s+", x))\
                            .filter(lambda x: len(x[1]) != 0 and len(x)-1> 0)
    #print(processing_rdd.take(5))
    #split friend list as single value
    SingleList_rdd=FriendList_rdd.map(lambda x: (x[0],re.split(r"\D+", x[1])))
    #print(SingleList_rdd.take(5))
    #pair
    pair_rdd=SingleList_rdd.flatMap(pair)
    #print(pair_rdd.take(5))
    group_rdd=pair_rdd.groupByKey()
    #print(group_rdd.take(5))
    result_rdd=group_rdd.map(number)\
                       .filter(lambda x:x[1]>0)\
                       .sortBy(lambda x: x[1], ascending=False)
    #print(result_rdd.take(5))

    #output file
    result_rdd.saveAsTextFile("./common_friends")