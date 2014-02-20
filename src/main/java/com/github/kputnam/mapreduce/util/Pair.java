package com.github.kputnam.mapreduce.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kputnam on 2/19/14.
 */
public class Pair<A extends Writable,
                  B extends Writable>
        implements Writable {

    public A fst;
    public B snd;

    /** Factory method benefits from type inference */
    public static <A extends Writable,
                   B extends Writable>
        Pair<A, B> build(A fst, B snd) {
        return new Pair(fst, snd);
    }

    public Pair(A fst, B snd) {
        this.fst = fst;
        this.snd = snd;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        fst.write(out);
        snd.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fst.readFields(in);
        snd.readFields(in);
    }
}
