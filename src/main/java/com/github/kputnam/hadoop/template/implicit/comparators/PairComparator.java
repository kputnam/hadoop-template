package com.github.kputnam.hadoop.template.implicit.comparators;

import com.github.kputnam.hadoop.template.implicit.Pair;
import org.apache.hadoop.io.Writable;

import java.util.Comparator;

/**
 * Created by kputnam on 4/9/14.
 */
public class PairComparator<A extends Writable & Comparable<? super A>,
                            B extends Writable & Comparable<? super B>>
        implements Comparator<Pair<A, B>> {

    @Override
    public int compare(Pair<A, B> a, Pair<A, B> b) {
        int cmp = cmpNull(a.fst, b.fst);

        if (cmp == 0 && a.fst != null)
            cmp = a.fst.compareTo(b.fst);

        if (cmp == 0)
            cmp = cmpNull(a.snd, b.snd);

        if (cmp == 0 && a.snd != null)
            cmp = a.snd.compareTo(b.snd);

        return cmp;
    }

    private static int cmpNull(Object a, Object b) {
        if (a != null && b == null)
            return 1;

        if (a == null && b != null)
            return -1;

        return 0;
    }
}
