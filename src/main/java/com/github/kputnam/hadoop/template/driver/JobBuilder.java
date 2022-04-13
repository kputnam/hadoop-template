package com.github.kputnam.hadoop.template.driver;

import com.github.kputnam.hadoop.template.util.MagicUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Constructs a Job instance and provides compile-time type safety that
 * guarantees the parts fit together correctly. Unspecified parameters
 * to Job will fallback to the Job class's defaults.
 *
 * For instance if you do not provide a Mapper or Reducer class, your Job
 * will use the identity mapper and reducer (which copy input records to
 * their output, without transformation).
 *
 * Beware you can NOT provide a polymorphic class (with un-instantiated
 * type parameters) to any methods in this class. For example, you must
 * subclass and instantiate the type parameters for SequenceFileOutputFormat
 *
 * This won't compile, because K & V must be instantiated:
 *
 *      withOutput(SequenceFileOutputFormat.class)
 *
 * But this will, because MyOutput is monomorphic:
 *
 *     class MyOutput extends SequenceFileOutputFormatClass&lt;Text, Text&gt; { }
 *
 *     withOutput(MyOutput.class)
 *
 * Created by kputnam on 5/9/14.
 *
 * @param <K1> Key from input source
 * @param <V1> Val from input source
 * @param <K2> Intermediate key sent from Mapper to Reducer
 * @param <V2> Intermediate val sent from Mapper to Reducer
 * @param <K3> Key written to output
 * @param <V3> Val written to output
 */
public class JobBuilder<K1, V1, K2, V2, K3, V3> {

    private Class<? extends InputFormat<K1, V1>> inputClass;
    private Class<? extends Mapper<K1, V1, K2, V2>> mapperClass;
    private Class<K2> mapperOutKeyClass;
    private Class<V2> mapperOutValClass;
    private Class<? extends RawComparator<? super K2>> mapperOutKeySortComparator;
    private Class<? extends RawComparator<? super K2>> mapperOutKeyGroupComparator;
    private Class<? extends Partitioner<K2, V2>> partitionerClass;
    private Class<? extends Reducer<K2, V2, K2, V2>> combinerClass;
    private Class<? extends Reducer<K2, V2, K3, V3>> reducerClass;
    private Class<K3> reducerOutKeyClass;
    private Class<V3> reducerOutValClass;
    private Class<? extends OutputFormat<K3, V3>> outputClass;

    private JobBuilder(Class<K2> mapperOutKeyClass,
                       Class<V2> mapperOutValClass,
                       Class<K3> reducerOutKeyClass,
                       Class<V3> reducerOutValClass) {
        this.mapperOutKeyClass  = mapperOutKeyClass;
        this.mapperOutValClass  = mapperOutValClass;
        this.reducerOutKeyClass = reducerOutKeyClass;
        this.reducerOutValClass = reducerOutValClass;
    }

    // Default comparator, and infer K1, V1, K2, V2, K3, V3
    @SuppressWarnings("unchecked")
    public static <K1, V1, K2 extends WritableComparable, V2, K3, V3>
        JobBuilder<K1, V1, K2, V2, K3, V3>
    newInstance(Class<? extends InputFormat<K1, V1>> inputClass,
                Class<? extends Mapper<K1, V1, K2, V2>> mapperClass,
                Class<? extends Reducer<K2, V2, K3, V3>> reducerClass) {
        return newInstance(
                inputClass,
                (Class<K2>) MagicUtil.getTypeParam(mapperClass, 2),
                (Class<V2>) MagicUtil.getTypeParam(mapperClass, 3),
                (Class<K3>) MagicUtil.getTypeParam(reducerClass, 2),
                (Class<V3>) MagicUtil.getTypeParam(reducerClass, 3))
            .withMapper(mapperClass)
            .withReducer(reducerClass);
    }

    // Default comparator
    public static <K1, V1, K2 extends WritableComparable, V2, K3, V3>
        JobBuilder<K1, V1, K2, V2, K3, V3>
    newInstance(Class<? extends InputFormat<K1, V1>> inputClass,
                Class<K2> mapperOutKeyClass,
                Class<V2> mapperOutValClass,
                Class<K3> reducerOutKeyClass,
                Class<V3> reducerOutValClass) {
        return new JobBuilder<K1, V1, K2, V2, K3, V3>(
                mapperOutKeyClass,
                mapperOutValClass,
                reducerOutKeyClass,
                reducerOutValClass)
            .withInput(inputClass);
    }

    // Comparator required, infer K1, V1, K2, V2, K3, V3
    @SuppressWarnings("unchecked")
    public static <K1, V1, K2, V2, K3, V3>
        JobBuilder<K1, V1, K2, V2, K3, V3>
    newInstance(Class<? extends InputFormat<K1, V1>> inputClass,
                Class<? extends Mapper<K1, V1, K2, V2>> mapperClass,
                Class<? extends RawComparator<? super K2>> mapperOutKeyComparator,
                Class<? extends Reducer<K2, V2, K3, V3>> reducerClass) {
        return newInstance(
                inputClass,
                (Class<K2>) MagicUtil.getTypeParam(mapperClass, 2),
                (Class<V2>) MagicUtil.getTypeParam(mapperClass, 3),
                mapperOutKeyComparator,
                (Class<K3>) MagicUtil.getTypeParam(reducerClass, 2),
                (Class<V3>) MagicUtil.getTypeParam(reducerClass, 3))
            .withMapper(mapperClass)
            .withReducer(reducerClass);
    }

    // Comparator required
    public static <K1, V1, K2, V2, K3, V3>
        JobBuilder<K1, V1, K2, V2, K3, V3>
    newInstance(Class<? extends InputFormat<K1, V1>> inputClass,
                Class<K2> mapperOutKeyClass,
                Class<V2> mapperOutValClass,
                Class<? extends RawComparator<? super K2>> mapperOutKeyComparator,
                Class<K3> reducerOutKeyClass,
                Class<V3> reducerOutValClass) {
        return new JobBuilder<K1, V1, K2, V2, K3, V3>(
                mapperOutKeyClass,
                mapperOutValClass,
                reducerOutKeyClass,
                reducerOutValClass)
            .withInput(inputClass)
            .withMapperOutKeySortComparator(mapperOutKeyComparator)
            .withMapperOutKeyGroupComparator(mapperOutKeyComparator);
    }

    public JobBuilder<K1, V1, K2, V2, K3, V3> withInput(Class<? extends InputFormat<K1, V1>> inputClass) {
        this.inputClass = inputClass;
        return this;
    }

    public JobBuilder<K1, V1, K2, V2, K3, V3> withMapper(Class<? extends Mapper<K1, V1, K2, V2>> mapperClass) {
        this.mapperClass = mapperClass;
        return this;
    }

    public JobBuilder<K1, V1, K2, V2, K3, V3> withReducer(Class<? extends Reducer<K2, V2, K3, V3>> reducerClass) {
        this.reducerClass = reducerClass;
        return this;
    }

    public JobBuilder<K1, V1, K2, V2, K3, V3> withMapperOutKeySortComparator(Class<? extends RawComparator<? super K2>> mapperOutKeySortComparator) {
        this.mapperOutKeySortComparator = mapperOutKeySortComparator;
        return this;
    }

    public JobBuilder<K1, V1, K2, V2, K3, V3> withMapperOutKeyGroupComparator(Class<? extends RawComparator<? super K2>> mapperOutKeyGroupComparator) {
        this.mapperOutKeyGroupComparator = mapperOutKeyGroupComparator;
        return this;
    }

    public JobBuilder<K1, V1, K2, V2, K3, V3> withCombiner(Class<? extends Reducer<K2, V2, K2, V2>> combinerClass) {
        this.combinerClass = combinerClass;
        return this;
    }

    public JobBuilder<K1, V1, K2, V2, K3, V3> withPartitioner(Class<? extends Partitioner<K2, V2>> partitionerClass) {
        this.partitionerClass = partitionerClass;
        return this;
    }

    public JobBuilder<K1, V1, K2, V2, K3, V3> withOutput(Class<? extends OutputFormat<K3, V3>> outputClass) {
        this.outputClass = outputClass;
        return this;
    }

    public Job build(Configuration conf) throws IOException {
        Job job = new Job(conf);
        job.setJarByClass(getClass());

        // Defaults to TextInputFormat.class
        if (inputClass != null)
            job.setInputFormatClass(inputClass);

        // Defaults to Mapper.class
        if (mapperClass != null)
            job.setMapperClass(mapperClass);

        job.setMapOutputKeyClass(mapperOutKeyClass);
        job.setMapOutputValueClass(mapperOutValClass);

        // Defaults to WritableComparator.get(mapperOutKeyClass)
        if (mapperOutKeySortComparator != null)
            job.setSortComparatorClass(mapperOutKeySortComparator);

        // Defaults to WritableComparator.get(mapperOutKeyClass)
        if (mapperOutKeyGroupComparator != null)
            job.setGroupingComparatorClass(mapperOutKeyGroupComparator);

        // Defaults to null
        if (combinerClass != null)
            job.setCombinerClass(combinerClass);

        // Defaults to HashPartitioner.class
        if (partitionerClass != null)
            job.setPartitionerClass(partitionerClass);

        // Defaults to Reducer.class
        if (reducerClass != null)
            job.setReducerClass(reducerClass);

        // Defaults to TextOutputFormat.class
        if (outputClass != null)
            job.setOutputFormatClass(outputClass);

        job.setOutputKeyClass(reducerOutKeyClass);
        job.setOutputValueClass(reducerOutValClass);

        return job;
    }
}
