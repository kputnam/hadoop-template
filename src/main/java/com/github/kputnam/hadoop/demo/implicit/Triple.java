package com.github.kputnam.hadoop.demo.implicit;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Created by kputnam on 2/20/14.
 */
public abstract class Triple<A extends Writable,
                             B extends Writable,
                             C extends Writable>
    implements Writable {

    public A fst;
    public B snd;
    public C thd;

    // Metadata (serialization overhead)
    private static final int NOT_NULL = 0x0;
    private static final int FST_NULL = 0x1;
    private static final int SND_NULL = 0x2;
    private static final int THD_NULL = 0x4;

    public Triple() { }

    public Triple(A fst, B snd, C thd) {
        this.fst = fst;
        this.snd = snd;
        this.thd = thd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Triple that = (Triple) o;
        return !(this.fst == null ^ that.fst == null)
            && !(this.snd == null ^ that.snd == null)
            && !(this.thd == null ^ that.thd == null)
            && (this.fst == null || this.fst.equals(that.fst))
            && (this.snd == null || this.snd.equals(that.snd))
            && (this.thd == null || this.thd.equals(that.thd));
    }

    @Override
    public int hashCode() {
        return 31 * (fst != null ? fst.hashCode() : 0)
             + 31 * (snd != null ? snd.hashCode() : 0)
                  + (thd != null ? thd.hashCode() : 0);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Indicate which fields are null
        out.writeByte((fst == null ? FST_NULL : NOT_NULL)
                     |(snd == null ? SND_NULL : NOT_NULL)
                     |(thd == null ? THD_NULL : NOT_NULL));

        // Serialize each non-null field
        if (fst != null) fst.write(out);
        if (snd != null) snd.write(out);
        if (thd != null) thd.write(out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        fst = null;
        snd = null;
        thd = null;

        int flags = in.readByte();

        try {
            if ((flags & FST_NULL) == NOT_NULL) {
                fst = (A) getClassA().newInstance();
                fst.readFields(in);
            }

            if ((flags & SND_NULL) == NOT_NULL) {
                snd = (B) getClassB().newInstance();
                snd.readFields(in);
            }

            if ((flags & THD_NULL) == NOT_NULL) {
                thd = (C) getClassC().newInstance();
                thd.readFields(in);
            }
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    @SuppressWarnings("unchecked")
    private Class<A> getClassA() {
        Type klass = getClass().getGenericSuperclass();
        Type param = ((ParameterizedType) klass).getActualTypeArguments()[0];
        return (Class<A>) param;
    }

    @SuppressWarnings("unchecked")
    private Class<B> getClassB() {
        Type klass = getClass().getGenericSuperclass();
        Type param = ((ParameterizedType) klass).getActualTypeArguments()[1];
        return (Class<B>) param;
    }

    @SuppressWarnings("unchecked")
    private Class<B> getClassC() {
        Type klass = getClass().getGenericSuperclass();
        Type param = ((ParameterizedType) klass).getActualTypeArguments()[2];
        return (Class<B>) param;
    }
}
