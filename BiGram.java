//Input -> gs://b929542-coc105/input/
//Output -> gs://b929542-coc105/output/

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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

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
            int reducer = 0;
            final String partitionKey = key.toString().substring(0, 1);
            final String[] regex = {"[^A-Z0-9]", "[0-9]", "[A-E]", "[F-J]", "[K-O]", "[P-T]", "[U-Z]"};

            for (int i = 0; i < regex.length; i++) {
                reducer = 0;
                Pattern pattern = Pattern.compile(regex[i], Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(partitionKey);
                
                if (matcher.matches()) {
                    reducer = i;
                    break;
                }
            }

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

    // public static class BGComparator extends WritableComparator {
    //     protected BGComparator() {
    //         super(LetterWritable.class, true);
    //     }

    //     public int compare(WritableComparable w1, WritableComparable w2) {
    //         LetterWritable k1 = (LetterWritable) w1;
    //         LetterWritable k2 = (LetterWritable) w2;

    //         return k1.compareTo(k2);
    //     }
    // }
  
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "bigram");
        job.setJarByClass(BiGram.class);
        job.setMapperClass(BGMapper.class);
        job.setReducerClass(BGReducer.class);
        job.setCombinerClass(BGReducer.class);
        job.setPartitionerClass(BGPartitioner.class);
        // job.setSortComparatorClass(BGComparator.class);
        job.setNumReduceTasks(7);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
