package com.github.kputnam.hadoop.demo.implicit.comparators;

import com.github.kputnam.hadoop.demo.implicit.Triple;
import org.apache.hadoop.io.Writable;

import java.util.Comparator;

/**
 * Created by kputnam on 4/9/14.
 */
public class TripleComparator <A extends Writable & Comparable<? super A>,
                               B extends Writable & Comparable<? super B>,
                               C extends Writable & Comparable<? super C>>
        implements Comparator<Triple<A, B, C>> {

    @Override
    public int compare(Triple<A, B, C> a, Triple<A, B, C> b) {
        int cmp = cmpNull(a.fst, b.fst);

        if (cmp == 0 && a.fst != null)
            cmp = a.fst.compareTo(b.fst);

        if (cmp == 0)
            cmp = cmpNull(a.snd, b.snd);

        if (cmp == 0 && a.snd != null)
            cmp = a.snd.compareTo(b.snd);

        if (cmp == 0)
            cmp = cmpNull(a.trd, b.trd);

        if (cmp == 0 && a.trd != null)
            cmp = a.trd.compareTo(b.trd);

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
