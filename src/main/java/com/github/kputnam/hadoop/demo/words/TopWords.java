package com.github.kputnam.hadoop.demo.words;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * Computes the answer to "which words occur exactly N times?"
 *
 * Created by kputnam on 2/10/14.
 */
public class TopWords extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TopWords(), args));
    }

    @Override
    public int run(String[] rest) throws Exception {
        if (rest.length != 2)
            throw new RuntimeException("Expected two args to TopWords#run([...])");

        String inPath = rest[0];
        String outPath = rest[1];

        Job job = new Job(getConf(), "topwords");
        job.setJarByClass(TopWords.class);

        job.setInputFormatClass(input.class);
        input.addInputPath(job, new Path(inPath));

        job.setMapperClass(mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(reducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(output.class);
        output.setOutputPath(job, new Path(outPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Note: Our input is lines of "word\tfreq". Unlike WordCount, now we use the frequency
     * as the key, and the word as the value. One possible way to implement this is using
     * TextInputFormat, which will emit (pos, "word\tfreq") pairs, discard the position
     * and parse "word\freq" into ("word", freq) pairs in the Mapper.
     *
     * The other way is by implementing a custom InputFormat, like so, where we parse the
     * "word\tfreq" into a ("word", freq) pair, and the Mapper reverses them.
     *
     * @see com.github.kputnam.hadoop.demo.words.Histogram.input and Histogram.mapper for
     * an even more concise way to implement this.
     */
    public static class input extends FileInputFormat<Text, IntWritable> {
        @Override
        public RecordReader<Text, IntWritable> createRecordReader(InputSplit in, TaskAttemptContext ctx)
                throws IOException, InterruptedException {
            return new reader();
        }

        public static class reader extends RecordReader<Text, IntWritable> {
            private Text word;
            private IntWritable freq;
            private LineRecordReader reader;

            @Override
            public void initialize(InputSplit in, TaskAttemptContext ctx) throws IOException, InterruptedException {
                word = new Text("");
                freq = new IntWritable(0);
                reader = new LineRecordReader();
                reader.initialize(in, ctx);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if (!reader.nextKeyValue())
                    return false;

                String[] parts = reader.getCurrentValue().toString().split("\t", 2);
                word.set(parts[0]);
                freq.set(Integer.parseInt(parts[1]));

                return true;
            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return word;
            }

            @Override
            public IntWritable getCurrentValue() throws IOException, InterruptedException {
                return freq;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return reader.getProgress();
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        }
    }

    public static class mapper extends Mapper<Text, IntWritable, IntWritable, Text> {
        @Override
        protected void map(Text word, IntWritable freq, Context ctx)
                throws IOException, InterruptedException {

            if (freq.get() < 5)
                return; // Skip a large number of highly uncommon words

            ctx.write(freq, word);
        }
    }

    /**
     * Reduce
     *   (1, "a")
     *   (1, "i")
     *   (1, "o")
     *   (2, "it")
     *   (2, "of")
     *   ...
     *
     * To
     *   (1, "a, i, o")
     *   (2, "it, of")
     *   ...
     */
    public static class reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable freq, Iterable<Text> words, Context ctx)
                throws IOException, InterruptedException {
            Set<String> set = new TreeSet<String>();
            StringBuilder b = new StringBuilder();

            for (Text word: words)
                set.add(word.toString());

            for (String word: set) {
                b.append(word);
                b.append(", ");
            }

            if (b.length() > 0)
                b.setLength(b.length() - 2); // Remove trailing ", "

            ctx.write(freq, new Text(b.toString()));
        }
    }

    public static class output extends TextOutputFormat<IntWritable, Text> {
        @Override
        public RecordWriter<IntWritable, Text> getRecordWriter(TaskAttemptContext job)
                throws IOException, InterruptedException {
            return super.getRecordWriter(job);
        }
    }

}