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
            String sentence = value.toString().replaceAll("\\p{P}", "");
            //StringTokenizer itr = new StringTokenizer(sentence);  
            String words[];
            words = sentence.split("\\s+");
            
            // while (itr.hasMoreTokens()) {
            //     words.add(itr.nextToken());
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

    public class TOPartitioner extends Partitioner<Text,IntWritable,Text,IntWritable> {
        @Override
        public int getPartition(CompositeKey key, DonationWritable value, int numPartitions) {
    
            if (key.state.compareTo("E") < 0) {
                return 0;
            } else if (key.state.compareTo("I") < 0) {
                return 1;
            } if (key.state.compareTo("M") < 0) {
                return 2;
            } else if (key.state.compareTo("Q") < 0) {
                return 3;
            } if (key.state.compareTo("U") < 0) {
                return 4;
            } else if (key.state.compareTo("X") < 0) {
                return 5;
            } else {
                return 6;
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
      job.setPartitionerClass(TOPartitioner.class);
      job.setNumReduceTasks(7);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
