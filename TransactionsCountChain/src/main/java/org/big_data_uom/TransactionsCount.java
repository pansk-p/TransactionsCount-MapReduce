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
    public static class TokenizerMapper1 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer lineToken = new StringTokenizer(value.toString(), " ");
            ArrayList<String> lineArray = new ArrayList<String>();
            while (lineToken.hasMoreElements()) {
                lineArray.add((lineToken.nextToken()));
            }
            Collections.sort(lineArray);
            word.set(lineArray.toString());
            context.write(word, one);
        }
    }
    public static class TokenizerMapper2 extends Mapper<Object, Text, Text, IntWritable> {
        private static IntWritable count = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer lineToken = new StringTokenizer(value.toString(), ",[]\t  ", false);
            ArrayList<String> lineArray = new ArrayList<String>();
            while (lineToken.hasMoreElements()) {
                lineArray.add((lineToken.nextToken()));
            }
            int lastIndex = lineArray.size() - 1;
            count.set(Integer.parseInt(lineArray.get(lastIndex)));
            lineArray.remove(lastIndex);
            int n = lineArray.size();
            if (n < 1) {
                return;
            }
            for (int i = 1; i < (1 << n); i++) {
                int m = 1;
                ArrayList<String> arrForWords = new ArrayList<String>();
                for (int j = 0; j < n; j++) {
                    if ((i & m) > 0) {
                        arrForWords.add(lineArray.get(j));
                    }
                    m = m << 1;
                }
                //Collections.sort(arrForWords);
                word.set(arrForWords.toString());
                context.write(word, count);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String trans = context.getConfiguration().get("threshold");
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
        conf.set("threshold", args[4]);

        // JOB 1
        Job job1 = Job.getInstance(conf, "transactionsCount-job1");
        job1.setJarByClass(TransactionsCount.class);
        job1.setMapperClass(TokenizerMapper1.class);
        job1.setCombinerClass(Combiner.class);
        job1.setReducerClass(Combiner.class);
        job1.setNumReduceTasks(Integer.parseInt(args[5]));
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        job1.waitForCompletion(true);

        // JOB 2
        Job job2 = Job.getInstance(conf, "transactionsCount-job2");
        job2.setJarByClass(TransactionsCount.class);
        job2.setMapperClass(TokenizerMapper2.class);
        job2.setCombinerClass(Combiner.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setNumReduceTasks(Integer.parseInt(args[5]));
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}