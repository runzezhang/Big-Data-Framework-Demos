import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Hadoop MapReduce
//Question 4:  Yelp Businesses Rating Located in “Palo Alto”
//Runze Zhang
//rxz160630@utdallas.edu
//Test Environment: Ubuntu 16.04 LTS
//                  MacBook Pro (13-inch, 2017, Four Thunderbolt 3 Ports)
//                  IntelliJ IDEA 2017.2.5
//                  java version "1.8.0_144"

public class PaloAlto_Review {
    public static class usersAndRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        HashSet<String> business_idSet = new HashSet<String>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String businessDataFile = conf.get("File");
            // File location in HDFS
            Path part = new Path(businessDataFile);

            FileSystem filesys = FileSystem.get(conf);
            FileStatus[] fsstaus = filesys.listStatus(part);
            for (FileStatus status : fsstaus) {
                Path pt = status.getPath();

                BufferedReader br = new BufferedReader(new InputStreamReader(filesys.open(pt)));
                String line = br.readLine();
                while (line != null) {

                    String[] data = line.split("::");
                    if (data.length > 2 && data[1].contains("Palo Alto")) {

                        business_idSet.add(data[0]);
                    }
                    line = br.readLine();
                }

            }
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("::");
            if (data.length > 3 && business_idSet.contains(data[2])) {

                context.write(new Text(data[1]), new Text(data[3]));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
        if (otherArgs.length < 2) {
            System.err.println("Jar <review.csv directory> <business.csv directory> <output directory>");
            System.exit(2);
        }

        conf.set("File", otherArgs[1]);

        Job job = new Job(conf, "Palo Alto Review");
        job.setJarByClass(PaloAlto_Review.class);

        job.setMapperClass(usersAndRatingMapper.class);
        //No reducer
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));



        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
