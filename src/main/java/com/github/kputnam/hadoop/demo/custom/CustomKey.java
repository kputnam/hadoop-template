package com.github.kputnam.hadoop.demo.custom;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Objects which can be sent over the network as keys must implement
 * the WritableComparable interface to define not only serialization
 * and deserialization, but also how to order instances for sorting
 * records being sent to a Reducer.
 *
 * Sometimes keys can be compared directly using their byte sequences
 * without fully deserializing them into objects. This can be more
 * efficient than the default mechanism of deserializing both keys and
 * then calling CustomKey#compareTo(). The implementation of WritableComparator
 * demonstrates how the default mechanism works.
 *
 * http://developer.yahoo.com/hadoop/tutorial/module5.html#keytypes
 */
public class CustomKey implements WritableComparable<CustomKey> {

    public float x;
    public float y;
    public float z;

    public CustomKey(float x, float y, float z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    @Override
    public int compareTo(CustomKey that) {
        return Float.compare(this.magnitude(), that.magnitude());
    }

    private float magnitude() {
        return (float) Math.sqrt(x*x + y*y + z*z);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) return true;
        if (that == null || getClass() != that.getClass()) return false;

        return this.x == ((CustomKey) that).x
            && this.y == ((CustomKey) that).y
            && this.z == ((CustomKey) that).z;
    }

    @Override
    public int hashCode() {
        int result = (x != +0.0f ? Float.floatToIntBits(x) : 0);
        result = 31 * result + (y != +0.0f ? Float.floatToIntBits(y) : 0);
        result = 31 * result + (z != +0.0f ? Float.floatToIntBits(z) : 0);
        return result;
    }

    /*
     * These methods are declared by Writable
     */

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(x);
        out.writeFloat(y);
        out.writeFloat(z);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        x = in.readFloat();
        y = in.readFloat();
        z = in.readFloat();
    }

    // Register this comparator
    static { WritableComparator.define(CustomKey.class, new Comparator()); }

    // Essentially the default implementation for any WritableComparable
    public static class Comparator extends WritableComparator {
        final private DataInputBuffer aBuffer = new DataInputBuffer();
        final private DataInputBuffer bBuffer = new DataInputBuffer();
        final private CustomKey aKey = new CustomKey(0f, 0f, 0f);
        final private CustomKey bKey = new CustomKey(0f, 0f, 0f);

        public Comparator() { super(CustomKey.class); }

        @Override
        public int compare(byte[] aBytes, int aStart, int aLength,
                           byte[] bBytes, int bStart, int bLength) {
            aBuffer.reset(aBytes, aStart, aLength);
            bBuffer.reset(bBytes, bStart, bLength);

            try {
                aKey.readFields(aBuffer);
                bKey.readFields(bBuffer);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return aKey.compareTo(bKey);
        }
    }

}
