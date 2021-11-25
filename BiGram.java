//Input -> gs://coc105/input/
//Output -> gs://coc105/output/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class BiGram {

    public static class BGMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text bigram = new Text();
  
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String sentence = value.toString().replaceAll("\\p{P}", "");
            sentence = sentence.trim().replaceAll("\\s+", " ");
            String words[];
            words = sentence.split(" ");

            for (int i = 0; i < words.length; i++) {
                if (i < words.length-1) {
                    bigram.set(words[i] + " " + words[i+1]);
                    context.write(bigram, one);
                }
            }
        }
    }

    public static class BGPartitioner extends Partitioner<Text,IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            // final String regex1 = "/[^A-Z0-9]/ig";
            // final String regex2 = "/[0-9]/g";
            // final String regex3 = "/[A-E]/ig";
            // final String regex4 = "/[F-J]/ig";
            // final String regex5 = "/[K-O]/ig";
            // final String regex6 = "/[P-T]/ig";
            // final String regex7 = "/[V-Z]/ig";
            int reducer;
            final String partitionKey = key.toString().substring(0, 1);
            final String[] regex = {"/[^A-Z0-9]/ig", "/[0-9]/g", "/[A-E]/ig", "/[F-J]/ig", "/[K-O]/ig", "/[P-T]/ig", "/[V-Z]/ig"};

            for (int i = 0; i < regex.length; i++) {
                Pattern pattern = Pattern.compile(regex[i]);
                Matcher matcher = pattern.matcher(partitionKey);
                
                if (matcher.matches()) {
                    reducer = i;
                    break;
                }
            }

            // if (Pattern.matches("/[^A-Z0-9]/ig", partitionKey)) {
            //     return 0;
            // } else if (Pattern.matches("/[0-9]/g", partitionKey)) {
            //     return 1;
            // } else if (Pattern.matches("/[A-E]/ig", partitionKey)) {
            //     return 2;
            // } else if (Pattern.matches("/[F-J]/ig", partitionKey)) {
            //     return 3;
            // } else if (Pattern.matches("/[K-O]/ig", partitionKey)) {
            //     return 4;
            // } else if (Pattern.matches("/[P-T]/ig", partitionKey)) {
            //     return 5;
            // } else if (Pattern.matches("/[V-Z]/ig", partitionKey)) {
            //     return 6;
            // } else {
            //     return 0;
            // }

            return reducer;
        }
    }
  
    public static class BGReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
  
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }
  
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "bigram");
      job.setJarByClass(BiGram.class);
      job.setMapperClass(BGMapper.class);
      job.setReducerClass(BGReducer.class);
      job.setCombinerClass(BGReducer.class);
      job.setPartitionerClass(BGPartitioner.class);
      job.setNumReduceTasks(7);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
