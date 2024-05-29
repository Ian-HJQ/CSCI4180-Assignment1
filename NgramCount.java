import java.io.EOFException;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NgramCount {
    
    public static class CustomInputFormat extends TextInputFormat{
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            // System.out.println("isSplitable is being called");
            return false;
        }
    }

    public static class TokenizerMapper 
            extends Mapper<Object, Text, Text, IntWritable>{
            
            private final static IntWritable one = new IntWritable(1);    
            private Text word = new Text();
            private IntWritable length = new IntWritable();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                int N = Integer.parseInt(conf.get("N"));
                String str = new String(value.toString().replaceAll("[\\p{Punct}\\n]+", " "));
                str = str.replaceAll("\\t", "");
                str = str.replaceAll("^\\s+", "");
                String[] itr = str.split("\\s+");

                for (int i=0; i<itr.length; i++) {
                    if (i+N-1 < itr.length){
                        String tmp = new String(itr[i]);
                        for(int j=1; j<N; j++){
                            if (i+j < itr.length){
                                tmp = tmp + " " + itr[i+j];
                            }
                        }
                        word.set(tmp);
                        context.write(word, one);
                    }   
                }
            }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable>{
            
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
        // conf.set("fs.defaultFS", "file:///");
        // conf.set("mapreduce.framework.name", "local");
        conf.set("N", args[2]);
        conf.set("textinputformat.record.delimiter", "\n \n \n");
        Job job = Job.getInstance(conf, "ngram count");
        job.setJarByClass(NgramCount.class);
        job.setInputFormatClass(CustomInputFormat.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}