package com.github.kputnam.hadoop.demo.words;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class Histogram extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2)
            usage();

        String inputPath  = args[0];
        String outputPath = args[1];

        Job job = new Job(getConf(), "histogram");
        job.setJarByClass(Histogram.class);

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(combiner.class);
        job.setReducerClass(reducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //

        job.setInputFormatClass(input.class);
        input.addInputPath(job, new Path(inputPath));

        job.setOutputFormatClass(output.class);
        output.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void usage() {
        System.err.println("usage: hadoop -jar <...> histogram <input> <output>");
        System.exit(-1);
    }

    /**
     * This built-in input format reads a "key\tval" pair from each line
     * and emits them as Text. We can then parse the Text into the correct
     * data types in the Mapper, below.
     */
    public static class input extends KeyValueTextInputFormat {

    }

    /**
     * Reduce
     *   (1, 1)     -- one word shows up once
     *   (1, 1)
     *   (1, 10)    -- ten words appear once
     *   (2, 3)
     *   (2, 5)     -- five words appear twice
     *   ...
     *
     * To
     *   (1, 12)    -- twelve words appear exactly once
     *   (2, 8)     -- eight words appear exactly twice
     *   ...
     */
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
    public static class output extends TextOutputFormat<IntWritable, IntWritable> {

    }

}
