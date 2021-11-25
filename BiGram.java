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

    // public class TOPartitioner {
    //     public static void main(String[] args) throws Exception {
     
    //         // Create job and parse CLI parameters
    //         Job job = Job.getInstance(new Configuration(), "Total Order Sorting example");
    //         job.setJarByClass(TotalOrderPartitionerExample.class);
            
    //         Path inputPath = new Path(args[0]);
    //         Path partitionOutputPath = new Path(args[1]);
    //         Path outputPath = new Path(args[2]);
     
    //         // The following instructions should be executed before writing the partition file
    //         job.setNumReduceTasks(3);
    //         FileInputFormat.setInputPaths(job, inputPath);
    //         TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionOutputPath);
    //         job.setInputFormatClass(KeyValueTextInputFormat.class);
    //         job.setMapOutputKeyClass(Text.class);
            
    //         // Write partition file with random sampler
    //         InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
    //         InputSampler.writePartitionFile(job, sampler);
     
    //         // Use TotalOrderPartitioner and default identity mapper and reducer 
    //         job.setPartitionerClass(TotalOrderPartitioner.class);
    //         job.setMapperClass(Mapper.class);
    //         job.setReducerClass(Reducer.class);
     
    //         FileOutputFormat.setOutputPath(job, outputPath);
    //         System.exit(job.waitForCompletion(true) ? 0 : 1);
    //     }
    // }
  
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

      job.setNumReduceTasks(7);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      TotalOrderPartitioner.setPartitionFile(job.getConfiguration, new Path(args[1]));
      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setMapOutputKeyClass(Text.class);

      InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
      InputSampler.writePartitionFile(job, sampler);

      job.setMapperClass(BGMapper.class);
      job.setReducerClass(BGReducer.class);
      job.setCombinerClass(BGReducer.class);

      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
