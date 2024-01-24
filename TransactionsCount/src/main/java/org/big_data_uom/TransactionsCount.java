package org.big_data_uom;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class TransactionsCount extends Configured implements Tool {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itrLine = new StringTokenizer(value.toString(), " ");
            ArrayList<String> arrForLine = new ArrayList<String>();
            while (itrLine.hasMoreElements()) {
                arrForLine.add((itrLine.nextToken()));
            }
            int n = arrForLine.size();
            if (n < 1) {
                return;
            }
            for (int i = 1; i < (1 << n); i++) {
                int m = 1;
                ArrayList<String> arrForWords = new ArrayList<String>();
                for (int j = 0; j < n; j++) {
                    if ((i & m) > 0) {
                        arrForWords.add(arrForLine.get(j));
                    }
                    m = m << 1;
                }
                Collections.sort(arrForWords);
                word.set(arrForWords.toString());
                context.write(word, one);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String trans = context.getConfiguration().get("transactions");
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= Integer.parseInt(trans)) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        int res = ToolRunner.run(new TransactionsCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("transactions", args[3]);

        Job job = Job.getInstance(conf, "transactionsCount");
        job.setJarByClass(TransactionsCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(Integer.parseInt(args[4]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}