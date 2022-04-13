package com.github.kputnam.hadoop.template.implicit;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Created by kputnam on 2/20/14.
 */
public abstract class Pair<A extends Writable,
                           B extends Writable>
    implements Writable {

    public A fst;
    public B snd;

    private static final int NOT_NULL = 0x0;
    private static final int FST_NULL = 0x1;
    private static final int SND_NULL = 0x2;

    public Pair() { }

    public Pair(A fst, B snd) { this.fst = fst; this.snd = snd; }

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
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('(');
        b.append(fst);
        b.append(',');
        b.append(snd);
        b.append(')');
        return b.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Indicate which fields are null
        out.writeByte((fst == null ? FST_NULL : NOT_NULL)
                     |(snd == null ? SND_NULL : NOT_NULL));

        // Serialize each non-null field
        if (fst != null) fst.write(out);
        if (snd != null) snd.write(out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        this.fst = null;
        this.snd = null;

        final byte flags = in.readByte();

        try {
            if ((flags & FST_NULL) == NOT_NULL) {
                fst = (A) getTypeParam(0).newInstance();
                fst.readFields(in);
            }

            if ((flags & SND_NULL) == NOT_NULL) {
                snd = (B) getTypeParam(1).newInstance();
                snd.readFields(in);
            }
        } catch (Exception e) { throw new IOException(e); }
    }

    @SuppressWarnings("unchecked")
    private Class<?> getTypeParam(int n) {
        Type klass = getClass().getGenericSuperclass();
        Type param = ((ParameterizedType) klass).getActualTypeArguments()[n];
        return (Class<?>) param;
    }
}
