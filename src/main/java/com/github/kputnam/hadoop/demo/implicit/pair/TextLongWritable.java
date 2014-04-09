package com.github.kputnam.hadoop.demo.implicit.pair;

import com.github.kputnam.hadoop.demo.implicit.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by kputnam on 4/9/14.
 */
public class TextLongWritable extends Pair<Text, LongWritable> {
    public static TextLongWritable of(Text fst, LongWritable snd) {
        return new TextLongWritable(fst, snd);
    }

    public TextLongWritable() {
        super();
    }

    public TextLongWritable(Text fst, LongWritable snd) {
        super(fst, snd);
    }
}
