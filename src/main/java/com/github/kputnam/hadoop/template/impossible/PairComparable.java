package com.github.kputnam.hadoop.template.impossible;

import org.apache.hadoop.io.WritableComparable;

/**
 * Created by kputnam on 2/19/14.
 */
public class PairComparable<A extends WritableComparable,
                            B extends WritableComparable>
        extends Pair<A, B>
        implements WritableComparable<PairComparable<A, B>> {

    public static <A extends WritableComparable,
                   B extends WritableComparable>
        Pair<A, B> of(A fst, B snd) {
        return new Pair(fst, snd);
    }

    public PairComparable(A fst, B snd) {
        super(fst, snd);
    }

    @Override
    public int compareTo(PairComparable<A, B> that) {
        int comparison = this.fst.compareTo(that.fst);
        if (comparison == 0)
            return this.snd.compareTo(that.snd);

        return comparison;
    }
}
