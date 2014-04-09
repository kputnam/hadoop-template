package com.github.kputnam.hadoop.demo.implicit.pair;

import com.github.kputnam.hadoop.demo.implicit.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by kputnam on 4/9/14.
 */
public class LongWritableText extends Pair<LongWritable, Text> {
    public static LongWritableText of(LongWritable fst, Text snd) {
        return new LongWritableText(fst, snd);
    }

    public LongWritableText() {
        super();
    }

    public LongWritableText(LongWritable fst, Text snd) {
        super(fst, snd);
    }
}
