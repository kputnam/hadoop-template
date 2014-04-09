package com.github.kputnam.hadoop.demo.implicit.pair;

import com.github.kputnam.hadoop.demo.implicit.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by kputnam on 4/9/14.
 */
public class LongWritableText extends Pair<LongWritable, Text> {
    public LongWritableText() {
        super();
    }

    public LongWritableText(LongWritable fst, Text snd) {
        super(fst, snd);
    }
}
