//Input Directory -> gs://b929542-coc105/input/
//Output Directory -> gs://b929542-coc105/output/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.*;

public class BiGram {

    public static class BG implements WritableComparable<BG> {
        private Text key;

        public BG() {
            set(new Text());
        }

        public BG(Text key) {
            set(new Text(key));
        }

        public void set(Text key) {
            this.key = key;
        }
    
        public void write(DataOutput output) throws IOException {
            key.write(output);
        }
        
        public void readFields(DataInput input) throws IOException {
            key.readFields(input);
        }

        public Text get() {
            return key;
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;

            result = prime * result + ((key == null) ? 0 : key.hashCode());

            return result;
        }
          
        public int compareTo(BG o) {
            String thisKey = this.key.toString();
            String thatKey = o.key.toString();

            return (thisKey.compareTo(thatKey) != 0 ? -1 : (thisKey == thatKey ? 0 : 1));
        }
    }

    public static class BGMapper extends Mapper<Object, Text, BG, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private BG bigram = new BG();
  
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String words[];
            String text = value.toString().replaceAll("[^a-zA-Z0-9 ]", "");
            words = text.trim().split("\\s+");

            for (int i = 0; i < words.length; i++) {
                if (i < words.length-1) {
                    bigram.set(new Text(words[i] + " " + words[i+1]));
                    context.write(bigram, one);
                }
            }
        }
    }

    public static class BGCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
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

    public static class BGReducer extends Reducer<BG,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
  
        public void reduce(BG key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(new Text(key.get()), result);
        }
    }

    public static class BGPartitioner extends Partitioner<BG,IntWritable> {
        public int getPartition(BG key, IntWritable value, int numReduceTasks) {
            int reducer = 0;
            final String partitionKey = key.get().toString().substring(0, 1);
            final String[] regex = {"[0-9]", "[A-D]", "[E-H]", "[I-L]", "[M-P]", "[Q-U]", "[V-Z]"};

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

    public static class BGSortComparator extends WritableComparator {
        public BGSortComparator() {
            super(BG.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            BG a_key = (BG) a;
            BG b_key = (BG) b;

            String thisKey = a_key.get().toString();
            String thatKey = b_key.get().toString();

            return thisKey.toLowerCase().compareTo(thatKey.toLowerCase());
        }
    }
  
    public static class BGReducer extends Reducer<BG,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
  
        public void reduce(BG key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(new Text(key.get()), result);
        }
    }
  
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "bigram");
        job.setJarByClass(BiGram.class);
        job.setMapperClass(BGMapper.class);
        job.setReducerClass(BGReducer.class);
        job.setCombinerClass(BGCombiner.class);
        job.setPartitionerClass(BGPartitioner.class);
        job.setSortComparatorClass(BGSortComparator.class);
        job.setNumReduceTasks(7);
        job.setOutputKeyClass(BG.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
