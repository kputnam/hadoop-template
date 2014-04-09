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
public abstract class Set<A extends Writable>
        implements Writable {

    public java.util.Set<A> set;

    public Set() { }

    public Set(java.util.Set<A> set) { this.set = set; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Set that = (Set) o;
        return !(this.set == null ^ that.set == null)
            && !(this.set == null || this.set.equals(that.set));
    }

    @Override
    public int hashCode() {
        return set != null ? set.hashCode() : 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(set.size());

        for (A element: set)
            element.write(out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        try {
            set = (java.util.Set<A>) getSetClass().newInstance();

            for (int count = in.readInt(); count > 0; count --) {
                A element = (A) getTypeParam(0).newInstance();
                element.readFields(in);

                set.add(element);
            }
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    protected abstract Class<Set<A>> getSetClass();

    @SuppressWarnings("unchecked")
    private Class<?> getTypeParam(int n) {
        Type klass = getClass().getGenericSuperclass();
        Type param = ((ParameterizedType) klass).getActualTypeArguments()[n];
        return (Class<?>) param;
    }
}
