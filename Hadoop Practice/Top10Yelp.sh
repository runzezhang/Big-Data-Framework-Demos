rm -rf Top10_yelp_output
hdfs dfs -rm -r -f /parallels/Temp
hdfs dfs -rm -r -f /parallels/Top10_yelp_output

hadoop jar ~/Documents/Hadoop_jar/Top10businesses.jar /parallels/input/review.csv /parallels/input/business.csv  /parallels/Temp /parallels/Top10_yelp_output
hdfs dfs -get /parallels/Top10_yelp_output



