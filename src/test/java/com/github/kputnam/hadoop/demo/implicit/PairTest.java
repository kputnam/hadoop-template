package com.github.kputnam.hadoop.demo.implicit;

import com.github.kputnam.hadoop.demo.implicit.pair.LongWritableText;
import com.github.kputnam.hadoop.demo.implicit.pair.TextLongWritable;
import org.apache.hadoop.io.*;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by kputnam on 2/20/14.
 */
public class PairTest {

    @Test
    public void testFields() {
        TextLongWritable p =
                new TextLongWritable(new Text("A"), new LongWritable(2));

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
        LongWritableText p = new LongWritableText(new LongWritable(3), new Text("A"));
        LongWritableText q = new LongWritableText();
        compareRoundtrip(p, q);
    }

    @Test
    public void testNulls() throws Exception {
        LongWritableText p = new LongWritableText(null, null);
        LongWritableText q = new LongWritableText();
        compareRoundtrip(p, q);
    }

    @Test
    public void testFstNull() throws Exception {
        LongWritableText p = new LongWritableText(null, new Text("A"));
        LongWritableText q = new LongWritableText();
        compareRoundtrip(p, q);
    }

    @Test
    public void testSndNull() throws Exception {
        LongWritableText p = new LongWritableText(new LongWritable(3), null);
        LongWritableText q = new LongWritableText();
        compareRoundtrip(p, q);
    }
}
