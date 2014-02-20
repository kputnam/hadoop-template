package com.github.kputnam.hadoop.demo.algebra;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kputnam on 2/19/14.
 */
public class Pair implements Writable {

    public DoubleWritable fst;
    public DoubleWritable snd;

    public static Pair of(DoubleWritable fst, DoubleWritable snd) {
        return new Pair(fst, snd);
    }

    public Pair() {
        this.fst = new DoubleWritable();
        this.snd = new DoubleWritable();
    }

    public Pair(DoubleWritable fst, DoubleWritable snd) {
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

    @Override
    public boolean equals(Object that) {
        if (this == that) return true;
        if (that == null || getClass() != that.getClass()) return false;

        final Pair pair = (Pair) that;
        return this.fst.equals(pair.fst)
            && this.snd.equals(pair.snd);
    }

    @Override
    public int hashCode() {
        return 31 * fst.hashCode() + snd.hashCode();
    }

    @Override
    public String toString() {
        return "(" + fst + ", " + snd + ")";
    }
}
