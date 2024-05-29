import java.io.EOFException;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NgramRF {
    
    public static class CustomInputFormat extends TextInputFormat{
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }
    }

    public static class TokenizerMapper 
            extends Mapper<Object, Text, Text, IntWritable>{
            
            private final static IntWritable one = new IntWritable(1);    
            private Text word = new Text();

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
                        context.write(new Text(itr[i] + " *"), one);
                    }   
                }
            }
    }

    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val:values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable>{
            
            private DoubleWritable freq = new DoubleWritable();
            private IntWritable total = new IntWritable();
            private Text currentWord = new Text("NO_CURRENT_WORD");
            
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            
                Configuration conf = context.getConfiguration();
                double theta = Double.parseDouble(conf.get("theta"));
                int count = 0;

                String[] tmp = key.toString().split("\\s");
                if (tmp[1].equals("*")){
                    if(!tmp[0].equals(currentWord.toString())){
                        currentWord.set(tmp[0]);
                        total.set(0);
                    }
                    for (IntWritable val : values){
                        total.set(total.get() + val.get());
                    }
                    //context.write(key, total);
                }
                else{
                    for (IntWritable val : values) {
                        count += val.get();
                    }
                    freq.set(((double)count)/total.get());
                    if (Double.compare(freq.get(), theta) >= 0){
                        context.write(key, freq);
                    }
                }
            }
    }

    public static class CustomPartitioner<K, V> extends Partitioner<K, V>{

        public int getPartition(K key, V value, int numReduceTasks){
            String left_key = new String(key.toString().split("\\s")[0]);
            return (left_key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "file:///");
        // conf.set("mapreduce.framework.name", "local");
        conf.set("N", args[2]);
        conf.set("theta", args[3]);
        conf.set("textinputformat.record.delimiter", "\n \n \n");
        Job job = Job.getInstance(conf, "ngram relative frequency");
        job.setJarByClass(NgramRF.class);
        job.setInputFormatClass(CustomInputFormat.class);
        job.setPartitionerClass(CustomPartitioner.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
