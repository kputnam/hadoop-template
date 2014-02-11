package com.github.kputnam.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.StringTokenizer;

public class Histogram implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        String[] rest = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (rest.length != 2)
            usage();

        Job job = new Job(conf);
        job.setJobName("histogram");
        job.setJarByClass(Histogram.class);

        job.setInputFormatClass(input.class);
        input.addInputPath(job, new Path(rest[0]));

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(combiner.class);
        job.setReducerClass(reducer.class);

        job.setOutputFormatClass(output.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        output.setOutputPath(job, new Path(rest[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void usage() {
        System.err.println("usage: hadoop -jar <...> histogram <input> <output>");
        System.exit(-1);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    //
    public static class input extends KeyValueTextInputFormat {

    }

    // Type params: input key, input value, output key, output value
    public static class mapper extends Mapper<Text, Text, IntWritable, IntWritable> {
        private IntWritable key = new IntWritable(0);
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(Text word, Text freq, Context ctx)
                throws IOException, InterruptedException {
            // One word with the given frequency
            key.set(Integer.parseInt(freq.toString()));
            ctx.write(key, one);
        }
    }

    //
    public static class combiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable freq, Iterable<IntWritable> counts, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count: counts)
                sum += count.get();
            ctx.write(freq, new IntWritable(sum));
        }
    }

    //
    public static class reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable freq, Iterable<IntWritable> counts, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count: counts)
                sum += count.get();
            ctx.write(freq, new IntWritable(sum));
        }
    }

    //
    public static class output extends TextOutputFormat {

    }

}
