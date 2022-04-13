package com.github.kputnam.hadoop.template.generic;

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
    private static final int NOT_NULL = 0x0;
    private static final int FST_NULL = 0x1;
    private static final int SND_NULL = 0x2;

    public Pair() { }

    public Pair(A fst, B snd) { this.fst = fst; this.snd = snd; }

    public static <A extends Writable, B extends Writable>
        Pair<A, B> of(A fst, B snd) { return new Pair<A, B>(fst, snd); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair that = (Pair) o;
        return !(this.fst == null ^ that.fst == null)
            && !(this.snd == null ^ that.snd == null)
            && (this.fst == null || this.fst.equals(that.fst))
            && (this.snd == null || this.snd.equals(that.snd));
    }

    @Override
    public int hashCode() {
        return 31 * (fst != null ? fst.hashCode() : 0)
                  + (snd != null ? snd.hashCode() : 0);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Serialize class names of each non-null field
        final ClassIndex index = new ClassIndex();

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
        this.fst = null;
        this.snd = null;

        final ClassIndex index = new ClassIndex();
        index.readFields(in);

        int flags = in.readByte();

        try {
            if ((flags & FST_NULL) == NOT_NULL) {
                fst = (A) index.getClass(in.readByte()).newInstance();
                fst.readFields(in);
            }

            if ((flags & SND_NULL) == NOT_NULL) {
                snd = (B) index.getClass(in.readByte()).newInstance();
                snd.readFields(in);
            }
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
