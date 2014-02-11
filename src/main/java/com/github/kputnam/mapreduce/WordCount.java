package com.github.kputnam.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        String[] rest = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (rest.length != 2)
            usage();

        Job job = new Job(conf);
        job.setJobName("word-count");
        job.setJarByClass(WordCount.class);

        job.setInputFormatClass(input.class);
        input.addInputPath(job, new Path(rest[0]));

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(combiner.class);
        job.setReducerClass(reducer.class);

        job.setOutputFormatClass(output.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        output.setOutputPath(job, new Path(rest[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void usage() {
        System.err.println("usage: hadoop -jar <...> wordcount <input> <output>");
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
    public static class input extends TextInputFormat {

    }

    // Type params: input key, input value, output key, output value
    public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text("");
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable _, Text line, Context ctx)
                throws IOException, InterruptedException {
            String delimiters = " \t\r\n\f~`!@#$%^&*()[{]}/?=+\\|-_'\",<.>;:";
            StringTokenizer tok = new StringTokenizer(line.toString(), delimiters);

            while (tok.hasMoreTokens()) {
                word.set(tok.nextToken());
                ctx.write(word, one);
            }
        }
    }

    //
    public static class combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text word, Iterable<IntWritable> counts, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count: counts)
                sum += count.get();
            ctx.write(word, new IntWritable(sum));
        }
    }

    //
    public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text word, Iterable<IntWritable> counts, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count: counts)
                sum += count.get();
            ctx.write(word, new IntWritable(sum));
        }
    }

    //
    public static class output extends TextOutputFormat {

    }

}
