package com.github.kputnam.hadoop.demo.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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
import java.util.concurrent.TimeUnit;

public class Metrics extends Configured implements Tool {

    static final MetricRegistry metrics = new MetricRegistry();

    static com.codahale.metrics.Meter     mMeter = metrics.meter("wc.meter");
    static com.codahale.metrics.Histogram mHisto = metrics.histogram("wc.histo");
    static com.codahale.metrics.Timer     mTimer = metrics.timer("wc.timer");
    static com.codahale.metrics.Counter   mCount = metrics.counter("wc.count");

    static final ConsoleReporter console =
            ConsoleReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.MILLISECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .outputTo(System.out)
                    .build();

    /*static final GraphiteReporter reporter =
            GraphiteReporter.forRegistry(metrics)
                    .prefixedWith("hadoop-demo")
                    .convertRatesTo(TimeUnit.MILLISECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(new Graphite(new InetSocketAddress("127.0.0.1", 2003)));*/


    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2)
            usage();

        String inputPath  = args[0];
        String outputPath = args[1];

        Job job = new Job(getConf(), "metrics");
        job.setJarByClass(Metrics.class);

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

        metrics.register("wc.gauge", new com.codahale.metrics.Gauge<Long>() {
            @Override
            public Long getValue() {
                return System.currentTimeMillis();
            }
        });

        console.start(5, TimeUnit.SECONDS);
        int status = job.waitForCompletion(true) ? 0 : 1;

        console.report();
        console.close();

        return status;
    }

    private void usage() {
        System.err.println("usage: hadoop -jar <...> wordcount <input> <output>");
        System.exit(-1);
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
            String delimiters = " \u00A0\t\r\n\f~`!@#$%^&*()[{]}/?=+\\|-_'\",<.>;:";
            StringTokenizer tok = new StringTokenizer(line.toString(), delimiters);

            while (tok.hasMoreTokens()) {
                word.set(tok.nextToken().toLowerCase());
                mHisto.update(word.getLength());
                mMeter.mark();
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
            Timer.Context timer = mTimer.time();

            for (IntWritable count: counts)
                sum += count.get();

            mCount.inc(sum);
            timer.stop();

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
    public static class output extends TextOutputFormat<Text, IntWritable> {

    }

}