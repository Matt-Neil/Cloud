//Input -> gs://coc105/input/
//Output -> gs://coc105/output/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

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
            // String sentence = value.toString().trim().replaceAll("\\s{2,}", " ");
            // sentence = sentence.replaceAll("\\p{P}", "");
            String sentence = value.toString().replaceAll("\\p{P}", "");
            sentence = sentence.trim().replaceAll("\\s+", " ");
            //StringTokenizer itr = new StringTokenizer(sentence, " ");  
            String words[];
            words = sentence.split(" ");
            // String previousToken = itr.hasMoreTokens() ? itr.nextToken() : "";
            
            // while (itr.hasMoreTokens()) {
            //     bigram.set(previousToken + " " + itr.nextToken());
            //     context.write(bigram, one);
            //     previousToken = itr.nextToken();
            // }

            // for (int i = 0; i < words.size(); i++) {
            //     if (i < words.size()-1) {
            //         bigram.set(words.get(i) + " " + words.get(i+1));
            //         context.write(bigram, one);
            //     }
            // }

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
            String partitionKey = key.toString().charAt(0);

            if (partitionKey.matches("/[^A-Z0-9]/ig")) {
                return 0;
            } else if (partitionKey.matches("/[0-9]/g")) {
                return 1;
            } if (partitionKey.matches("/[A-E]/ig")) {
                return 2;
            } else if (partitionKey.matches("/[F-J]/ig")) {
                return 3;
            } if (partitionKey.matches("/[K-O]/ig")) {
                return 4;
            } else if (partitionKey.matches("/[P-T]/ig")) {
                return 5;
            } else if (partitionKey.matches("/[V-Z]/ig")) {
                return 6;
            } else {
                return 0;
            }
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
