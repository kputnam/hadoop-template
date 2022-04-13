package com.github.kputnam.hadoop.template.implicit.either;

import com.github.kputnam.hadoop.template.implicit.Either;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by kputnam on 4/9/14.
 */
public class TextLongWritable extends Either<Text, LongWritable> {
    public TextLongWritable(Text left) {
        setLeft(left);
    }

    public TextLongWritable(LongWritable right) {
        setRight(right);
    }
}
