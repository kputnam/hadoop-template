package com.github.kputnam.hadoop.demo.implicit;

import com.github.kputnam.hadoop.demo.util.EnumUtil;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Created by kputnam on 4/9/14.
 */
public abstract class Either<A extends Writable,
                             B extends Writable>
    implements Writable {

    private A left;
    private B right;

    private enum V { L, R }
    private V variant;

    public boolean isLeft() {
        return variant == V.L;
    }

    public boolean isRight() {
        return variant == V.R;
    }

    public A getLeft() {
        if (variant != V.L)
            throw new IllegalStateException("isLeft is false");

        return this.left;
    }

    public B getRight() {
        if (variant != V.R)
            throw new IllegalStateException("isRight is false");

        return this.right;
    }

    public A getLeftOr(A otherwise) {
        return variant == V.L ? this.left : otherwise;
    }

    public B getRightOr(B otherwise) {
        return variant == V.R ? this.right : otherwise;
    }

    public void setLeft(A left) {
        this.variant = V.L;
        this.left    = left;
        this.right   = null;
    }

    public void setRight(B right) {
        this.variant = V.R;
        this.left    = null;
        this.right   = right;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Either that = (Either) o;
        return (this.variant == that.variant)
            && (this.right   == null || this.right.equals(that.right))
            && (this.left    == null || this.left.equals(that.left));
    }

    @Override
    public int hashCode() {
        return 31 * (left != null ? left.hashCode() : 0)
             + 31 * (right != null ? right.hashCode() : 0)
                  + EnumUtil.hashCode(this.variant);
    }

    @Override
    public String toString() {
        if (this.variant == null)
            return "Either()";

        StringBuilder b = new StringBuilder();
        b.append("Either.");

        switch (this.variant) {
            case L:
                b.append("L(");
                b.append(left);
                break;
            case R:
                b.append("R(");
                b.append(right);
                break;
        }

        b.append(')');
        return b.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(variant == null ? -1 : variant.ordinal());

        if (this.variant == V.L)
            this.left.write(out);

        if (this.variant == V.R)
            this.right.write(out);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        this.variant = null;
        this.left    = null;
        this.right   = null;

        final byte flag = in.readByte();
        if (flag == -1)
            return;

        this.variant = V.values()[flag];

        try {
            switch (this.variant) {
                case L:
                    this.left = (A) getTypeParam(0).newInstance();
                    this.left.readFields(in);
                    break;
                case R:
                    this.right = (B) getTypeParam(1).newInstance();
                    this.right.readFields(in);
                    break;
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
