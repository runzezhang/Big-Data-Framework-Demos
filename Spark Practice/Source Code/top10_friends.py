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
    conf = SparkConf().setAppName("top10_friends").setMaster("local")
    sc = SparkContext(conf=conf)
    #read files
    inputRDD1 = sc.textFile("soc-LiveJournal1Adj.txt",1)
    inputRDD2 = sc.textFile("userdata.txt",1)

    #split id and his/her friend list
    FriendList_rdd= inputRDD1.map(lambda x: re.split(r"\s+", x))\
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


    top10_result=sc.parallelize(result_rdd.take(10))
    #print(top10_result.take(5))
    user_rdd=inputRDD2.map(lambda x:(re.split(",", x)[0], (re.split(",", x)[1],re.split(",",x)[2],re.split(",",x)[3])))
    join1 = top10_result.map(lambda x:(x[0][0],(x[1],x[0]))).join(user_rdd).map(lambda x:x[1])
    join2 = top10_result.map(lambda x: (x[0][1], (x[1], x[0]))).join(user_rdd).map(lambda x: x[1])

    final_join_rdd=join1.join(join2)
    print(final_join_rdd.take(5))

    final=final_join_rdd.map(lambda x:"\t".join((str(x[0][0]),str(x[1][0][0]),str(x[1][0][1]),str(x[1][0][2]),str(x[1][1][0]),str(x[1][1][1]),str(x[1][1][2]))))
        #.map(lambda x:"\t".join(x[0][0],x[1][0][0],x[1][0][1],x[1][0][2],x[1][1][0],x[1][1][1],x[1][1][2]))
    print(final.take(5))
    #output file
    final.repartition(1).saveAsTextFile("./top10_friends")
