package com.github.kputnam.hadoop.template.words;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kputnam on 2/20/14.
 */
public class Cooccurrences extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static class bagMonoid implements Writable {
        public Map<String, Integer> bag;

        public static bagMonoid zero() { return new bagMonoid(); }

        public static bagMonoid concat(Iterable<bagMonoid> rows) {
            bagMonoid accum = zero();

            for (bagMonoid row: rows)
                for (Map.Entry<String, Integer> entry: row.bag.entrySet())
                    if (!accum.bag.containsKey(entry.getKey()))
                        accum.bag.put(entry.getKey(), entry.getValue());
                    else
                        accum.bag.put(entry.getKey(), entry.getValue() + accum.bag.get(entry.getKey()));

            return accum;
        }

        public bagMonoid() { bag = new HashMap<String, Integer>(); }

        public int add(String key) {
            return add(key, 1);
        }

        public int add(String key, int count) {
            if (count < 0)
                throw new IllegalArgumentException("count must be non-negative");

            Integer present = bag.get(key);
            if (present == null) {
                bag.put(key, count);
                return count;
            }

            bag.put(key, present + count);
            return present + count;
        }

        public void append(bagMonoid that) {
            for (Map.Entry<String, Integer> entry: that.bag.entrySet())
                add(entry.getKey(), entry.getValue());
        }

        public int remove(String key) {
            return remove(key, 1);
        }

        public int remove(String key, int count) {
            if (count < 0)
                throw new IllegalArgumentException("count must be non-negative");

            Integer present = bag.get(key);
            if (present == null)
                return 0;

            if (present == count)
                bag.remove(key);
            else
                bag.put(key, present - count);

            return present - count;
        }

        public boolean contains(String key) {
            return bag.containsKey(key)
                && bag.get(key) >= 1;
        }

        public int count(String key) {
            Integer cnt = bag.get(key);
            return cnt == null ? 0 : cnt;
        }

        public int size() {
            int size = 0;
            for (Integer cnt: bag.values())
                size += cnt;
            return size;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            // TODO
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            // TODO
        }
    }

    public static class mapper extends Mapper<LongWritable, Text, Text, bagMonoid> {
        @Override
        protected void map(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException {
            //
        }
    }
}
