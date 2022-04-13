package com.github.kputnam.hadoop.template.implicit.comparators;

import com.github.kputnam.hadoop.template.implicit.Quad;
import org.apache.hadoop.io.Writable;

import java.util.Comparator;

/**
 * Created by kputnam on 4/9/14.
 */
public class QuadComparator<A extends Writable & Comparable<? super A>,
                            B extends Writable & Comparable<? super B>,
                            C extends Writable & Comparable<? super C>,
                            D extends Writable & Comparable<? super D>>
        implements Comparator<Quad<A, B, C, D>> {

    @Override
    public int compare(Quad<A, B, C, D> a, Quad<A, B, C, D> b) {
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

        if (cmp == 0)
            cmp = cmpNull(a.fth, b.fth);

        if (cmp == 0 && a.fth != null)
            cmp = a.fth.compareTo(b.fth);

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
