package com.github.kputnam.hadoop.demo.generic;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kputnam on 2/20/14.
 */
public class Pair<A extends Writable,
                  B extends Writable>
    implements Writable {

    public A fst;
    public B snd;

    // Metadata (serialization overhead)
    private static int NOT_NULL = 0x0;
    private static int FST_NULL = 0x1;
    private static int SND_NULL = 0x2;
    private ClassIndex index = new ClassIndex();

    public Pair() { }
    public Pair(A fst, B snd) { this.fst = fst; this.snd = snd; }
    public static <A extends Writable, B extends Writable>
    Pair<A, B> of(A fst, B snd) { return new Pair<A, B>(fst, snd); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (fst != null ? !fst.equals(pair.fst) : pair.fst != null) return false;
        if (snd != null ? !snd.equals(pair.snd) : pair.snd != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return 31 * (fst != null ? fst.hashCode() : 0)
                  + (snd != null ? snd.hashCode() : 0);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Serialize class names of each non-null field
        if (fst != null) index.addClass(fst.getClass());
        if (snd != null) index.addClass(snd.getClass());
        index.write(out);

        // Indicate which fields are null
        out.writeByte((fst == null ? FST_NULL : NOT_NULL)
                     |(snd == null ? SND_NULL : NOT_NULL));

        // Serialize each non-null field
        if (fst != null) { out.writeByte(index.getId(fst.getClass())); fst.write(out); }
        if (snd != null) { out.writeByte(index.getId(snd.getClass())); snd.write(out); }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        index.readFields(in);
        int flags = in.readByte();

        try {
            if ((flags & FST_NULL) == 0) {
                fst = (A) index.getClass(in.readByte()).newInstance();
                fst.readFields(in);
            }

            if ((flags & SND_NULL) == 0) {
                snd = (B) index.getClass(in.readByte()).newInstance();
                snd.readFields(in);
            }
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
