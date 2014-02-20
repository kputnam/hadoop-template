package com.github.kputnam.hadoop.demo.statistics;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kputnam on 2/20/14.
 */
public class Mean extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static class meanMonoid implements Writable {
        public DoubleWritable sum;
        public IntWritable cnt;

        public meanMonoid() {
            this.sum = new DoubleWritable(0);
            this.cnt = new IntWritable(0);
        }

        public meanMonoid(double sum) {
            this.sum = new DoubleWritable(sum);
            this.cnt = new IntWritable(1);
        }

        public meanMonoid(double sum, int cnt) {
            this.sum = new DoubleWritable(sum);
            this.cnt = new IntWritable(cnt);
        }

        public static meanMonoid append(Iterable<meanMonoid> those) {
            double sum = 0d;
            int    cnt = 0;

            for (meanMonoid that: those) {
                sum += that.sum.get();
                cnt ++;
            }

            return new meanMonoid(sum, cnt);
        }

        public DoubleWritable mean() {
            return cnt.get() == 0 ?
                new DoubleWritable(0) :
                new DoubleWritable(sum.get() / cnt.get());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            sum.write(out);
            cnt.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sum.readFields(in);
            cnt.readFields(in);
        }
    }

    public static class mapper<K> extends Mapper<K, DoubleWritable, K, meanMonoid> {
        @Override
        protected void map(K key, DoubleWritable val, Context ctx) throws IOException, InterruptedException {
            ctx.write(key, new meanMonoid(val.get()));
        }
    }

    public static class combiner<K> extends Reducer<K, meanMonoid, K, meanMonoid> {
        @Override
        protected void reduce(K key, Iterable<meanMonoid> vals, Context ctx) throws IOException, InterruptedException {
            ctx.write(key, meanMonoid.append(vals));
        }
    }

    public static class reduce<K> extends Reducer<K, meanMonoid, K, DoubleWritable> {
        @Override
        protected void reduce(K key, Iterable<meanMonoid> vals, Context ctx) throws IOException, InterruptedException {
            ctx.write(key, meanMonoid.append(vals).mean());
        }
    }
}
