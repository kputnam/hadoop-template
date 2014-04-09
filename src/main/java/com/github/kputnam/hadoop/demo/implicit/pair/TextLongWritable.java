package com.github.kputnam.hadoop.demo.implicit.pair;

import com.github.kputnam.hadoop.demo.implicit.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by kputnam on 4/9/14.
 */
public class TextLongWritable extends Pair<Text, LongWritable> {
    public TextLongWritable() {
        super();
    }

    public TextLongWritable(Text fst, LongWritable snd) {
        super(fst, snd);
    }
}
