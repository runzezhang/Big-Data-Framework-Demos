rm -rf Common_friend_output
hdfs dfs -rm -r -f /parallels/Common_friend_output
hadoop jar ~/Documents/Hadoop_jar/Common_friend.jar /parallels/input/soc-LiveJournal1Adj.txt /parallels/Common_friend_output
hdfs dfs -get /parallels/Common_friend_output
