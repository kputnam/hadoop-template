package com.github.kputnam.mapreduce.words;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class Ngrams extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2)
            usage();

        String inputPath  = args[0];
        String outputPath = args[1];

        Job job = new Job(getConf(), "ngrams");
        job.setJarByClass(Ngrams.class);

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(combiner.class);
        job.setReducerClass(reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //

        job.setInputFormatClass(input.class);
        input.addInputPath(job, new Path(inputPath));

        job.setOutputFormatClass(output.class);
        output.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void usage() {
        System.err.println("usage: hadoop -jar <...> ngrams <input> <output>");
        System.exit(-1);
    }

    //
    public static class input extends TextInputFormat {

    }

    // Type params: input key, input value, output key, output value
    public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static int n = 3;
        private Text ngram = new Text("");
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable _, Text line, Context ctx)
                throws IOException, InterruptedException {
            String delimiters = " \u00A0\t\r\n\f~`!@#$%^&*()[{]}/?=+\\|-_'\",<.>;:";
            StringTokenizer tok = new StringTokenizer(line.toString(), delimiters);

            while (tok.hasMoreTokens()) {
                String word = tok.nextToken();
                int wordLen = word.length();

                for (int k = 0; k+n < wordLen; k ++) {
                    ngram.set(word.substring(k, k+n));
                    ctx.write(ngram, one);
                }
            }
        }
    }

    //
    public static class combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text ngram, Iterable<IntWritable> counts, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count: counts)
                sum += count.get();
            ctx.write(ngram, new IntWritable(sum));
        }
    }

    //
    public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text ngram, Iterable<IntWritable> counts, Context ctx)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count: counts)
                sum += count.get();
            ctx.write(ngram, new IntWritable(sum));
        }
    }

    //
    public static class output extends TextOutputFormat<Text, IntWritable> {

    }

}
