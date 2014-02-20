package com.github.kputnam.hadoop.demo.generic;

import org.apache.hadoop.io.*;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kputnam on 2/20/14.
 */
public class PairTest {

    @Test
    public void testFields() {
        Pair<Text, LongWritable> p =
                Pair.of(new Text("A"), new LongWritable(2));

        Assert.assertEquals(new Text("A"), p.fst);
        Assert.assertEquals(new LongWritable(2), p.snd);
    }

    private <A extends Writable, B extends Writable>
        void compareRoundtrip(Pair<A, B> from, Pair<A, B> to) throws Exception {
        DataOutputBuffer out = new DataOutputBuffer();
        from.write(out);

        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), out.getLength());
        to.readFields(in);

        Assert.assertEquals(from.fst, to.fst);
        Assert.assertEquals(from.snd, to.snd);
    }

    @Test
    public void testCopy() throws Exception {
        Pair<LongWritable, Text> p = Pair.of(new LongWritable(3), new Text("A"));
        Pair<LongWritable, Text> q = new Pair<LongWritable, Text>();
        compareRoundtrip(p, q);
    }

    @Test
    public void testNulls() throws Exception {
        Pair<LongWritable, Text> p = new Pair<LongWritable, Text>(null, null);
        Pair<LongWritable, Text> q = new Pair<LongWritable, Text>();
        compareRoundtrip(p, q);
    }

    @Test
    public void testFstNull() throws Exception {
        Pair<LongWritable, Text> p = Pair.of(null, new Text("A"));
        Pair<LongWritable, Text> q = new Pair<LongWritable, Text>();
        compareRoundtrip(p, q);
    }

    @Test
    public void testSndNull() throws Exception {
        Pair<LongWritable, Text> p = Pair.of(new LongWritable(3), null);
        Pair<LongWritable, Text> q = new Pair<LongWritable, Text>();
        compareRoundtrip(p, q);
    }
}
