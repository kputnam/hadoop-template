package com.github.kputnam.hadoop.template.driver;

import com.github.kputnam.hadoop.template.implicit.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kputnam on 5/9/14.
 */
public class JobBuilderTest {

    Configuration conf = new Configuration();

    @Test
    public void testDefaultComparatorBookends() throws Exception {
        JobBuilder<LongWritable, Text, IntWritable, Text, Text, IntWritable> builder =
            JobBuilder.newInstance(
                TextInputFormat.class,
                MapperBogus.class,
                ReducerBogus.class);

        Job job = builder.build(conf);

        // Constructor arguments
        Assert.assertEquals(TextInputFormat.class,
                job.getInputFormatClass());
        Assert.assertEquals(MapperBogus.class,
                job.getMapperClass());
        Assert.assertEquals(IntWritable.class,
                job.getMapOutputKeyClass());
        Assert.assertEquals(Text.class,
                job.getMapOutputValueClass());
        Assert.assertEquals(ReducerBogus.class,
                job.getReducerClass());
        Assert.assertEquals(Text.class,
                job.getOutputKeyClass());
        Assert.assertEquals(IntWritable.class,
                job.getOutputValueClass());

        // Defaults
        Assert.assertEquals(IntWritable.Comparator.class,
                job.getSortComparator().getClass());
        Assert.assertEquals(IntWritable.Comparator.class,
                job.getGroupingComparator().getClass());
        Assert.assertEquals(null,
                job.getCombinerClass());
        Assert.assertEquals(HashPartitioner.class,
                job.getPartitionerClass());
        Assert.assertEquals(TextOutputFormat.class,
                job.getOutputFormatClass());
    }

    @Test
    public void testRequiredComparatorBookends() throws Exception {
        JobBuilder<LongWritable, Text, Pair, IntWritable, Text, NullWritable> builder =
            JobBuilder.newInstance(
                InputText.class,
                MapperRequiresComparator.class,
                ComparatorPair.class,
                ReducerRequiresComparator.class);

        Job job = builder.build(conf);

        // Constructor arguments
        Assert.assertEquals(InputText.class,
                job.getInputFormatClass());
        Assert.assertEquals(MapperRequiresComparator.class,
                job.getMapperClass());
        Assert.assertEquals(Pair.class,
                job.getMapOutputKeyClass());
        Assert.assertEquals(IntWritable.class,
                job.getMapOutputValueClass());
        Assert.assertEquals(ComparatorPair.class,
                job.getSortComparator().getClass());
        Assert.assertEquals(ComparatorPair.class,
                job.getGroupingComparator().getClass());
        Assert.assertEquals(ReducerRequiresComparator.class,
                job.getReducerClass());
        Assert.assertEquals(Text.class,
                job.getOutputKeyClass());
        Assert.assertEquals(NullWritable.class,
                job.getOutputValueClass());

        // Defaults
        Assert.assertEquals(null,
                job.getCombinerClass());
        Assert.assertEquals(HashPartitioner.class,
                job.getPartitionerClass());
        Assert.assertEquals(TextOutputFormat.class,
                job.getOutputFormatClass());
    }

    @Test
    public void testDefaultComparator() throws Exception {
        JobBuilder<Text, IntWritable, IntWritable, Text, Text, IntWritable> builder =
            JobBuilder.newInstance(
                InputTextInt.class,
                IntWritable.class,
                Text.class,
                Text.class,
                IntWritable.class);

        Job job = builder.build(conf);

        // Constructor arguments
        Assert.assertEquals(InputTextInt.class,
                job.getInputFormatClass());
        Assert.assertEquals(IntWritable.class,
                job.getMapOutputKeyClass());
        Assert.assertEquals(Text.class,
                job.getMapOutputValueClass());
        Assert.assertEquals(Text.class,
                job.getOutputKeyClass());
        Assert.assertEquals(IntWritable.class,
                job.getOutputValueClass());

        // Defaults
        Assert.assertEquals(IntWritable.Comparator.class,
                job.getSortComparator().getClass());
        Assert.assertEquals(IntWritable.Comparator.class,
                job.getGroupingComparator().getClass());
        Assert.assertEquals(Mapper.class,
                job.getMapperClass());
        Assert.assertEquals(null,
                job.getCombinerClass());
        Assert.assertEquals(HashPartitioner.class,
                job.getPartitionerClass());
        Assert.assertEquals(Reducer.class,
                job.getReducerClass());
        Assert.assertEquals(TextOutputFormat.class,
                job.getOutputFormatClass());
    }

    @Test
    public void testRequiredComparator() throws Exception {
        JobBuilder<Text, IntWritable, Pair, Text, Text, IntWritable> builder =
            JobBuilder.newInstance(
                InputTextInt.class,
                Pair.class,
                Text.class,
                ComparatorPair.class,
                Text.class,
                IntWritable.class);

        Job job = builder.build(conf);

        // Constructor arguments
        Assert.assertEquals(InputTextInt.class,
                job.getInputFormatClass());
        Assert.assertEquals(Pair.class,
                job.getMapOutputKeyClass());
        Assert.assertEquals(Text.class,
                job.getMapOutputValueClass());
        Assert.assertEquals(ComparatorPair.class,
                job.getSortComparator().getClass());
        Assert.assertEquals(ComparatorPair.class,
                job.getGroupingComparator().getClass());
        Assert.assertEquals(Text.class,
                job.getOutputKeyClass());
        Assert.assertEquals(IntWritable.class,
                job.getOutputValueClass());

        // Defaults
        Assert.assertEquals(Mapper.class,
                job.getMapperClass());
        Assert.assertEquals(null,
                job.getCombinerClass());
        Assert.assertEquals(HashPartitioner.class,
                job.getPartitionerClass());
        Assert.assertEquals(Reducer.class,
                job.getReducerClass());
        Assert.assertEquals(TextOutputFormat.class,
                job.getOutputFormatClass());
    }

    @Test
    public void testWithOverrides() throws Exception {
        JobBuilder<LongWritable, Text, Text, Text, LongWritable, NullWritable> builder =
                JobBuilder.newInstance(
                        TextInputFormat.class,
                        Text.class,
                        Text.class,
                        LongWritable.class,
                        NullWritable.class);

        Assert.assertEquals(InputText.class,
                builder.withInput(InputText.class).build(conf).getInputFormatClass());

        Assert.assertEquals(MapperTextInput.class,
                builder.withMapper(MapperTextInput.class).build(conf).getMapperClass());

        Assert.assertEquals(ComparatorText.class,
                builder.withMapperOutKeySortComparator(ComparatorText.class)
                        .build(conf).getSortComparator().getClass());

        Assert.assertEquals(ComparatorText.class,
                builder.withMapperOutKeyGroupComparator(ComparatorText.class)
                        .build(conf).getGroupingComparator().getClass());

        Assert.assertEquals(CombinerText.class,
                builder.withCombiner(CombinerText.class).build(conf).getCombinerClass());

        Assert.assertEquals(PartitionerText.class,
                builder.withPartitioner(PartitionerText.class).build(conf).getPartitionerClass());

        Assert.assertEquals(ReducerNull.class,
                builder.withReducer(ReducerNull.class).build(conf).getReducerClass());

        Assert.assertEquals(OutputText.class,
                builder.withOutput(OutputText.class).build(conf).getOutputFormatClass());

        /**
         // These should not type check
         builder.withInput(InputTextInt.class);
         builder.withMapper(MapperBogus.class);
         builder.withReducer(ReducerBogus.class);

         // This should not type check because TextOutputFormat is polymorphic
         builder.withOutput(TextOutputFormat.class);

         // This should not type check because damnit Hadoop!
         builder.withMapperOutKeySortComparator(Text.Comparator.class);
         builder.withMapperOutKeyGroupingComparator(Text.Comparator.class);
         **/
    }

    /* "Fixture data" for the compiler
    **************************************************************************/

    public static class InputText extends
            TextInputFormat { }

    public static class OutputText extends
            TextOutputFormat<LongWritable, NullWritable> { }

    public static class InputTextInt extends
            SequenceFileInputFormat<Text, IntWritable> { }

    public static class MapperTextInput extends
            Mapper<LongWritable, Text, Text, Text> { }

    public static class MapperBogus extends
            Mapper<LongWritable, Text, IntWritable, Text> { }

    public static class MapperRequiresComparator extends
            Mapper<LongWritable, Text, Pair, IntWritable> { }

    public static class ComparatorText implements
            RawComparator<Text> {
        @Override
        public int compare(byte[] as, int aStart, int aLength,
                           byte[] bs, int bStart, int bLength) {
            return 0;
        }

        @Override
        public int compare(Text a, Text b) {
            return 0;
        }
    }

    public static class ComparatorPair implements
            RawComparator<Pair> {
        @Override
        public int compare(byte[] as, int aStart, int aLength,
                           byte[] bs, int bStart, int bLength) {
            return 0;
        }

        @Override
        public int compare(Pair a, Pair b) {
            return 0;
        }
    }

    public static class CombinerText extends
            Reducer<Text, Text, Text, Text> { }

    public static class PartitionerText extends
            Partitioner<Text, Text> {
        @Override
        public int getPartition(Text k, Text v, int i) {
            return 0;
        }
    }

    public static class ReducerNull extends
            Reducer<Text, Text, LongWritable, NullWritable> { }

    public static class ReducerBogus extends
            Reducer<IntWritable, Text, Text, IntWritable> { }

    public static class ReducerRequiresComparator extends
            Reducer<Pair, IntWritable, Text, NullWritable> { }
}
