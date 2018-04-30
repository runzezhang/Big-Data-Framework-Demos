rm -rf Top10_friend_output
hdfs dfs -rm -r -f /parallels/Top10_friend_output
hdfs dfs -rm -r -f /temp
hadoop jar ~/Documents/Hadoop_jar/Top10Friends.jar /parallels/input/soc-LiveJournal1Adj.txt /parallels/Top10_friend_output
hdfs dfs -get /parallels/Top10_friend_output
