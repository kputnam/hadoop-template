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
public abstract class Quad<A extends Writable,
                           B extends Writable,
                           C extends Writable,
                           D extends Writable>
    implements Writable {

    public A fst;
    public B snd;
    public C trd;
    public D fth;

    private static final int NOT_NULL = 0x0;
    private static final int FST_NULL = 0x1;
    private static final int SND_NULL = 0x2;
    private static final int TRD_NULL = 0x4;
    private static final int FTH_NULL = 0x8;

    public Quad() { }

    public Quad(A fst, B snd, C trd, D fth) {
        this.fst = fst;
        this.snd = snd;
        this.trd = trd;
        this.fth = fth;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Quad that = (Quad) o;
        return !(this.fst == null ^ that.fst == null)
            && !(this.snd == null ^ that.snd == null)
            && !(this.trd == null ^ that.trd == null)
            && !(this.fth == null ^ that.fth == null)
            && (this.fst == null || this.fst.equals(that.fst))
            && (this.snd == null || this.snd.equals(that.snd))
            && (this.trd == null || this.trd.equals(that.trd))
            && (this.fth == null || this.fth.equals(that.fth));
    }

    @Override
    public int hashCode() {
        return 31 * (fst != null ? fst.hashCode() : 0)
             + 31 * (snd != null ? snd.hashCode() : 0)
             + 31 * (fth != null ? fth.hashCode() : 0)
                  + (trd != null ? trd.hashCode() : 0);
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('(');
        b.append(fst);
        b.append(',');
        b.append(snd);
        b.append(',');
        b.append(trd);
        b.append(',');
        b.append(fth);
        b.append(')');
        return b.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Indicate which fields are null
        out.writeByte((fst == null ? FST_NULL : NOT_NULL)
                     |(snd == null ? SND_NULL : NOT_NULL)
                     |(trd == null ? TRD_NULL : NOT_NULL)
                     |(fth == null ? FTH_NULL : NOT_NULL));

        // Serialize each non-null field
        if (fst != null) fst.write(out);
        if (snd != null) snd.write(out);
        if (trd != null) trd.write(out);
        if (fth != null) fth.write(out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        fst = null;
        snd = null;
        trd = null;
        fth = null;

        int flags = in.readByte();

        try {
            if ((flags & FST_NULL) == NOT_NULL) {
                fst = (A) getTypeParam(0).newInstance();
                fst.readFields(in);
            }

            if ((flags & SND_NULL) == NOT_NULL) {
                snd = (B) getTypeParam(1).newInstance();
                snd.readFields(in);
            }

            if ((flags & TRD_NULL) == NOT_NULL) {
                trd = (C) getTypeParam(2).newInstance();
                trd.readFields(in);
            }

            if ((flags & FTH_NULL) == NOT_NULL) {
                fth = (D) getTypeParam(3).newInstance();
                fth.readFields(in);
            }
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @SuppressWarnings("unchecked")
    private Class<?> getTypeParam(int n) {
        Type klass = getClass().getGenericSuperclass();
        Type param = ((ParameterizedType) klass).getActualTypeArguments()[n];
        return (Class<?>) param;
    }
}
