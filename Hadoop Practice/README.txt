Hadoop MapReduce

Runze Zhang
rxz160630@utdallas.edu

Test Environment: Ubuntu 16.04 LTS
                  MacBook Pro (13-inch, 2017, Four Thunderbolt 3 Ports)
                  IntelliJ IDEA 2017.2.5
                  java version "1.8.0_144"


Practice Questions are as follows:
1.A MapReduce program in Hadoop that implements a simple “Mutual/Common friend list of two friends"
2.Find friend pairs whose common friend number are within the top-10 in all the pairs. 
3.List the business_id, full address and categories of the Top 10 businesses using the average ratings.
4.List the 'user id' and 'rating' of users that reviewed businesses located in “Palo Alto”




Before test,  do following steps like:
1.
	
2.input data files like:
	hdfs dfs -put <business.csv>
		e.g: hdfs dfs -put ~/Documents/input_files/business.csv /parallels/input
	hdfs dfs -put <review.csv>
		e.g: hdfs dfs -put ~/Documents/input_files/review.csv /parallels/input
	hdfs dfs -put <soc-LiveJournal1Adj.txt>
		e.g: hdfs dfs -put ~/Documents/input_files/soc-LiveJournal1Adj.txt /parallels/input
	hdfs dfs -put <user.csv>
		e.g: hdfs dfs -put ~/Documents/input_files/user.csv /parallels/input


Question1: Common Friends
	java Source Code: Common_friend.java
	Jar file: Common_friend.jar
	Hadoop Command guide:

		rm -rf <output location>
		hdfs dfs -rm -r -f <output location in HDFS>
		hadoop jar <jar file location>  <input soc-LiveJournal1Adj.txt location in HDFS> <output location in HDFS>
		hdfs dfs -get <output location>

		e.g:
		rm -rf Common_friend_output
		hdfs dfs -rm -r -f /parallels/Common_friend_output
		hadoop jar ~/Documents/Hadoop_jar/Common_friend.jar /parallels/input/soc-LiveJournal1Adj.txt /parallels/Common_friend_output
		hdfs dfs -get /parallels/Common_friend_output



Question2: Top 10 Common Friends
	java Source Code: Top 10.java
	Jar file: Top 10.jar
	Hadoop Command guide:

		rm -rf <output location>
		hdfs dfs -rm -r -f <output location in HDFS>
		hdfs dfs -rm -r -f <temp file location in HDFS: /temp(already write in java file) >
		hadoop jar <jar file location>  <input soc-LiveJournal1Adj.txt location in HDFS> <output location in HDFS>
		hdfs dfs -get <output location>

		e.g:
		rm -rf Top10_friend_output
		hdfs dfs -rm -r -f /parallels/Top10_friend_output
		hdfs dfs -rm -r -f /temp
		hadoop jar ~/Documents/Hadoop_jar/Top10Friends.jar /parallels/input/soc-LiveJournal1Adj.txt /parallels/Top10_friend_output
		hdfs dfs -get /parallels/Top10_friend_output


Question3: Yelp Top 10 Businesses (using the average ratings)
	java Source Code: Top10Yelp.java
	Jar file: Top10Yelp.jar
	Hadoop Command guide:

		rm -rf <output location>
		hdfs dfs -rm -r -f <output location in HDFS>
		hdfs dfs -rm -r -f <temp file location in HDFS >
		hadoop jar <jar file location>  <input review.csv location in HDFS> <input business.csv location in HDFS> <Temp file location in HDFS> <output location in HDFS>
		hdfs dfs -get <output location>

		e.g:
		rm -rf Top10_yelp_output
		hdfs dfs -rm -r -f /parallels/Temp
		hdfs dfs -rm -r -f /parallels/Top10_yelp_output
		hadoop jar ~/Documents/Hadoop_jar/Top10businesses.jar /parallels/input/review.csv /parallels/input/business.csv  /parallels/Temp /parallels/Top10_yelp_output
		hdfs dfs -get /parallels/Top10_yelp_output


Question4: Yelp Businesses Rating Located in “Palo Alto”
	java Source Code: PaloAlto_Review.java
	Jar file: PaloAlto_Review.jar
	Hadoop Command guide:

		rm -rf <output location>
		hdfs dfs -rm -r -f <output location in HDFS>
		hadoop jar <jar file location>  <input review.csv location in HDFS> <input business.csv location in HDFS>  <output location in HDFS>
		hdfs dfs -get <output location>

		e.g:
		rm -rf PaloAlto_output
		hdfs dfs -rm -r -f /parallels/PaloAlto_output
		hadoop jar ~/Documents/Hadoop_jar/PaloAlto_Review.jar /parallels/input/review.csv /parallels/input/business.csv  /parallels/PaloAlto_output
		hdfs dfs -get /parallels/PaloAlto_output

Or you can simply put Hadoop_jar folder and all .sh files in the same path(such as Documents), use the commands as follows:(path may different in different environments, please check in .sh files)
dash Common_Friends.sh
dash Top10friends.sh
dash Top10Yelp.sh
dash PaloAltoReview.sh

