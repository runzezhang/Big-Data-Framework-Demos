rm -rf PaloAlto_output
hdfs dfs -rm -r -f /parallels/PaloAlto_output

hadoop jar ~/Documents/Hadoop_jar/PaloAlto_Review.jar /parallels/input/review.csv /parallels/input/business.csv  /parallels/PaloAlto_output
hdfs dfs -get /parallels/PaloAlto_output
