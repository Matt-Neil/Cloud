//Input -> gs://b929542-coc105/input/
//Output -> gs://b929542-coc105/output/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;
import java.io.DataInput;
import java.io.DataOutput;

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

    public class SortAlphabet implements WritableComparable<SortAlphabet> {
        protected String key = new String();
    
        public String getKey() {
            return key;
        }
    
        public void setKey(String key) {
            this.key = key;
        }
    
        SortAlphabet(Text key) {
            this.key = key.toString();
        }
    
        SortAlphabet() {
        }
    
        @Override
        public void write(DataOutput d) throws IOException {
            d.writeUTF(key);
        }
    
        @Override
        public void readFields(DataInput di) throws IOException {
            key = di.readUTF();
        }
    
        @Override
        public int compareTo(SortAlphabet t) {
            String thiskey = this.key;
            String thatkey = t.key;
    
            return thiskey.compareTo(thatkey);
        }
    }

    public static class BGMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text bigram = new Text();
  
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String words[];
            String text = value.toString().replaceAll("[^a-zA-Z0-9 ]", "");
            words = text.trim().split("\\s+");

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

    public static class BGComparator extends WritableComparator {
        public BGComparator() {
            super(SortAlphabet.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            SortAlphabet a_key = (SortAlphabet) a;
            SortAlphabet b_key = (SortAlphabet) b;

            String thiskey = a_key.getKey();
            String thatkey = b_key.getKey();

            return thiskey.compareTo(thatkey);
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
        job.setSortComparatorClass(BGComparator.class);
        job.setNumReduceTasks(7);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
