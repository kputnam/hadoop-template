package com.github.kputnam.hadoop.template.algebra;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by kputnam on 2/19/14.
 */
public class DotProduct extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2)
            usage();

        String inputPath = args[0];
        String outputPath = args[1];

        Job job = new Job(getConf(), "dot-product");
        job.setJarByClass(DotProduct.class);

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setCombinerClass(reducer.class);
        job.setReducerClass(reducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        //

        job.setInputFormatClass(input.class);
        input.addInputPath(job, new Path(inputPath));

        job.setOutputFormatClass(output.class);
        output.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void usage() {
        System.err.println("usage: hadoop -jar <...> dotproduct <input> <output>");
        System.exit(-1);
    }

    //
    public static class input
            extends SequenceFileInputFormat<NullWritable, Pair> {

    }

    //
    public static class mapper
            extends Mapper<NullWritable, Pair, NullWritable, DoubleWritable> {
        DoubleWritable product = new DoubleWritable();

        @Override
        protected void map(NullWritable _, Pair pair, Context ctx) throws IOException, InterruptedException {
            product.set(pair.fst.get() * pair.snd.get());
            ctx.write(NullWritable.get(), product);
        }
    }

    //
    public static class reducer
            extends Reducer<NullWritable, DoubleWritable,
                            NullWritable, DoubleWritable> {
        @Override
        protected void reduce(NullWritable _, Iterable<DoubleWritable> terms, Context ctx) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable term: terms)
                sum += term.get();
            ctx.write(NullWritable.get(), new DoubleWritable(sum));
        }
    }

    //
    public static class output extends TextOutputFormat<NullWritable, DoubleWritable> {

    }
}
