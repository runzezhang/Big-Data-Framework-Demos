import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Hadoop MapReduce
//Question 3: Yelp Top 10 Businesses (using the average ratings)
//Runze Zhang
//rxz160630@utdallas.edu
//Test Environment: Ubuntu 16.04 LTS
//                  MacBook Pro (13-inch, 2017, Four Thunderbolt 3 Ports)
//                  IntelliJ IDEA 2017.2.5
//                  java version "1.8.0_144"

public class Top10Yelp {
    // Mapper: business id and rating
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

            String list[] = value.toString().split("::");
            //map to id and rating
            context.write(new Text(list[2]), new Text(list[3]));

        }
    }

    //Reducer for select top 10 rate business id
    public static class TopTenReducer extends Reducer<Text, Text, Text, Text> {

        private TreeMap<String, Float> map = new TreeMap<>();

        private RateComparator RComparator = new RateComparator(map);

        private TreeMap<String, Float> sortedMap = new TreeMap<String, Float>(RComparator);

        //output id with average rating

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            float sum = 0;
            float count = 0;

            for (Text value : values) {
                sum =sum+ Float.parseFloat(value.toString());
                count++;
            }

            float avgerageRating = new Float(sum / count);
            map.put(key.toString(), avgerageRating);
        }

        //Comparator
        class RateComparator implements Comparator<String> {

            TreeMap<String, Float> mMap;

            public RateComparator(TreeMap<String, Float> lMap) {
                this.mMap = lMap;
            }

            public int compare(String a, String b) {
                if (mMap.get(a) >= mMap.get(b)) {
                    return -1;
                } else {
                    return 1;
                }
            }
        }
        //select top 10
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            sortedMap.putAll(map);

            int countOfRating = 10;
            for (Entry<String, Float> entry : sortedMap.entrySet()) {
                if (countOfRating == 0) {
                    break;
                }
                context.write(new Text(entry.getKey()),new Text(String.valueOf(entry.getValue())));
                countOfRating--;
            }
        }
    }




    //TopTen Mapper
    public static class TopTenMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] detail = line.split("\t");
            String businessID = detail[0].trim();
            String businessRating = detail[1].trim();
            context.write(new Text(businessID), new Text("T1|" + businessID + "|" + businessRating));

        }
    }

    //Show details Mapper
    public static class DetailsMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String businessData[] = value.toString().split("::");
            context.write(new Text(businessData[0].trim()), new Text("T2|"
                    + businessData[0].trim() + "|" + businessData[1].trim()
                    + "|" + businessData[2].trim()));

        }
    }

    //reducer with id, details and rating
    public static class TopTenRatedBusinessDetailReducer extends Reducer<Text, Text, Text, Text> {
        private ArrayList<String> Details = new ArrayList<String>();
        private ArrayList<String> topTen = new ArrayList<String>();

        private static String splitter = "\\|";

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text text : values) {
                String value = text.toString();
                if (value.startsWith("T1")) {
                    topTen.add(value.substring(3));
                } else {
                    Details.add(value.substring(3));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (String topBusiness : topTen) {
                for (String detail : Details) {
                    String[] t1Split = topBusiness.split(splitter);
                    String t1BusinessId = t1Split[0].trim();

                    String[] t2Split = detail.split(splitter);
                    String t2BusinessId = t2Split[0].trim();

                    if (t1BusinessId.equals(t2BusinessId)) {
                        context.write(new Text(t1BusinessId), new Text(
                                t2Split[1] + "\t" + t2Split[2] + "\t"
                                        + t1Split[1]));
                        break;
                    }
                }
            }
        }
    }


    //Driver Program
    public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException {

        Configuration config1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config1, args).getRemainingArgs();

        if (otherArgs.length != 4) {
            System.err.println("hadoop jar <input1> <input2> <temp> <final_output>");
            System.exit(0);
        }
        //job for top 10 rating
        {
            Job job1 = Job.getInstance(config1, "TopTen");
            job1.setJarByClass(Top10Yelp.class);
            job1.setMapperClass(RatingMapper.class);
            job1.setReducerClass(TopTenReducer.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

            if(!job1.waitForCompletion(true))
                System.exit(1);
        }
        //job for Details
        {
            Configuration config2 = new Configuration();
            Job job2 = Job.getInstance(config2, "Details");
            job2.setJarByClass(Top10Yelp.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            MultipleInputs.addInputPath(job2, new Path(args[2]),
                    TextInputFormat.class, TopTenMapper.class);
            MultipleInputs.addInputPath(job2, new Path(args[1]),
                    TextInputFormat.class, DetailsMapper.class);

            job2.setReducerClass(TopTenRatedBusinessDetailReducer.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            job2.waitForCompletion(true);
        }
    }


}