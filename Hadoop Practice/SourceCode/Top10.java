
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//Hadoop MapReduce
//Question 2: Top 10 Common Friends
//Runze Zhang
//rxz160630@utdallas.edu
//Test Environment: Ubuntu 16.04 LTS
//                  MacBook Pro (13-inch, 2017, Four Thunderbolt 3 Ports)
//                  IntelliJ IDEA 2017.2.5
//                  java version "1.8.0_144"

public class Top10 {
    //Map all pairs
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), "\n");
            String line = null;
            String[] lineArray = null;
            String[] friendArray = null;
            String[] tempArray = null;
            while(it.hasMoreTokens()){
                line = it.nextToken();
                lineArray = line.split("\t");
                if(lineArray.length > 1) {
                    friendArray = lineArray[1].split(",");
                    tempArray = new String[2];
                    //always keep pair (a,b) a<b
                    for(int i = 0; i < friendArray.length; i++){
                        if( Integer.parseInt(friendArray[i]) >= Integer.parseInt(lineArray[0]) ) {
                            tempArray[1] = friendArray[i];
                            tempArray[0] = lineArray[0];
                        }
                        else {
                            tempArray[0] = friendArray[i];
                            tempArray[1] = lineArray[0];
                        }

                        context.write(new Text(tempArray[0] + "," + tempArray[1]+"\t"), new Text(lineArray[1]));
                    }
                }
            }
        }

    }
    //Reduce for count common friends numbers
    public static class Reduce extends Reducer<Text,Text,LongWritable,Text>{
        public void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            Text []ar = new Text[2];
            int index = 0;
            for(Text it:values)
                ar[index++] = new Text(it);
            String []list1 = ar[0].toString().split(",");
            String []list2 = ar[1].toString().split(",");
            String res = "";
            int count = 0;
            for(int i = 0; i < list1.length; i++) {
                for(int j = 0; j < list2.length; j++) {
                    if(list1[i].equals(list2[j]) && count == 0) {

                        res = res.concat(list1[i]);
                        count++;
                        break;
                    }
                    if(list1[i].equals(list2[j]) && count > 0) {

                        res = res.concat(","+list1[i]);
                        count++;
                        break;
                    }
                }
            }
                context.write(new LongWritable(count), key);

        }

    }
    //2nd Mappper
    public static class Top10Map extends Mapper<Text, Text, LongWritable, Text> {
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(new LongWritable(Long.parseLong(key.toString())), value);
        }

    }
    //2nd Reducer for select top 10
    public static class Top10Reduce extends Reducer<LongWritable, Text, Text, LongWritable>{
        int count=0;
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
             {
                for (Text value : values) {
                    //only write down the first 10 records
                    if(count<10) {
                        context.write(value, key);
                        count++;
                    }
                }
            }

        }

    }
    //Driver
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path tempPath=new Path("/temp");
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }
        //First Job
        {
            Job job = new Job(conf, "Common_friend_Count");

            job.setJarByClass(Top10.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, tempPath);

            if(!job.waitForCompletion(true))
                System.exit(1);
        }
        //Second Job
        {
            Job job = new Job(conf, "Common_friend_Top10");

            job.setJarByClass(Top10.class);
            job.setMapperClass(Top10Map.class);
            job.setReducerClass(Top10Reduce.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            FileInputFormat.addInputPath(job, tempPath);
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }


    }
}
