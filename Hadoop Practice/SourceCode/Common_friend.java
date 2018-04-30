
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//Hadoop MapReduce
//Question 1: Common Friends
//Runze Zhang
//rxz160630@utdallas.edu
//Test Environment: Ubuntu 16.04 LTS
//                  MacBook Pro (13-inch, 2017, Four Thunderbolt 3 Ports)
//                  IntelliJ IDEA 2017.2.5
//                  java version "1.8.0_144"

public class Common_friend {

    //Mapper
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            String line = null;
            String[] lineList = null;
            String[] friendList = null;
            String[] tempArray = null;
            while(it.hasMoreTokens()){
                line = it.nextToken();
                lineList = line.split("\t");
                if(lineList.length > 1) {
                    friendList = lineList[1].split(",");
                    tempArray = new String[2];
                    for(int i = 0; i < friendList.length; i++){
                        if( Integer.parseInt(friendList[i]) >= Integer.parseInt(lineList[0]) ) {
                            tempArray[1] = friendList[i];
                            tempArray[0] = lineList[0];
                        }
                        else {
                            tempArray[0] = friendList[i];
                            tempArray[1] = lineList[0];
                        }

                        context.write(new Text(tempArray[0] + "," + tempArray[1]+"\t"), new Text(lineList[1]));
                    }
                }
            }
        }

    }
    //Reducer
    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //input specific pair to check
            ArrayList<String> input = new ArrayList<>();
            input.add("0,4	");
            input.add("20,22939	");
            input.add("1,29826	");
            input.add("6222,19272	");
            input.add("28041,28056	");
            Text []ar = new Text[2];
            int index = 0;
            for(Text it:values)
                ar[index++] = new Text(it);
            String []pair1 = ar[0].toString().split(",");
            String []pair2 = ar[1].toString().split(",");
            String list = "";
            int count = 0;
            for(int i = 0; i < pair1.length; i++) {
                for(int j = 0; j < pair2.length; j++) {
                    if(pair1[i].equals(pair2[j]) && count == 0) {

                        list = list.concat(pair1[i]);
                        count++;
                        break;
                    }
                    if(pair1[i].equals(pair2[j]) && count > 0) {

                        list = list.concat(","+pair1[i]);
                        count++;
                        break;
                    }
                }
            }
            if(input.contains(key.toString()))
            context.write( key, new Text(list) );

        }

    }

    //Driver Program
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Common_friend <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Common_friend");

        job.setJarByClass(Common_friend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
