package com.github.kputnam.hadoop.template.custom;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Objects which can be sent across the network must implement the
 * Writable interface to define how to serialize and deserialize
 * data. Hadoop provides Text, IntWritable, FloatWritable, etc.
 *
 * http://developer.yahoo.com/hadoop/tutorial/module5.html#writable
 */
public class CustomVal implements Writable {

    public float x;
    public float y;
    public float z;

    public CustomVal(float x, float y, float z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

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

}
