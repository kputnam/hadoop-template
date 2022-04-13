package com.github.kputnam.hadoop.template.words;

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

        job.setMapperClass(charMapper.class);
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
    public static class charMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static int n = 3;
        private Text ngram = new Text("");
        private IntWritable one = new IntWritable(1);

        public static void setN(int n) { charMapper.n = n; }

        @Override
        protected void map(LongWritable _, Text line, Context ctx)
                throws IOException, InterruptedException {
            String delimiters = " \u00A0\t\r\n\f~`!@#$%^&*()[{]}/?=+\\|-_'\",<.>;:";
            StringTokenizer tok = new StringTokenizer(line.toString(), delimiters);

            while (tok.hasMoreTokens()) {
                String word = tok.nextToken();
                int wordLen = word.length();

                for (int k = 0; k+n <= wordLen; k ++) {
                    ngram.set(word.substring(k, k+n));
                    ctx.write(ngram, one);
                }
            }
        }
    }

    //
    public static class wordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text ngram = new Text("");
        private IntWritable one = new IntWritable(1);

        private static int n = 3;
        private long bufferIdx = 0;
        private String[] buffer = null;
        private StringBuilder sb = new StringBuilder();

        public static void setN(int n) { charMapper.n = n; }

        @Override
        protected void map(LongWritable _, Text line, Context ctx)
                throws IOException, InterruptedException {
            if (buffer == null)
                buffer = new String[n];

            String delimiters = " \u00A0\t\r\n\f~`!@#$%^&*()[{]}/?=+\\|-_'\",<.>;:";
            StringTokenizer tok = new StringTokenizer(line.toString(), delimiters);

            while (tok.hasMoreTokens()) {
                // Shift in a new token with trailing whitespace
                buffer[(int) (bufferIdx++ % n)] = tok.nextToken() + " ";

                if (bufferIdx < n)
                    continue;

                // Copy n-word buffer to output
                sb.setLength(0);
                for (int k = 0; k < n; k ++)
                    sb.append(buffer[((int) (k + bufferIdx)) % n]);

                sb.setLength(sb.length() - 1); // Remove trailing " "
                ngram.set(sb.toString());
                ctx.write(ngram, one);
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
