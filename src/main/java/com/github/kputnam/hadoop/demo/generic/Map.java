package com.github.kputnam.hadoop.demo.generic;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by kputnam on 2/20/14.
 */
public class Map<K extends Writable, V extends Writable> implements Writable {
    public java.util.Map<K, V> map;

    // Metadata (serialization overhead)
    private ClassIndex index = new ClassIndex();

    public Map() { }
    public Map(java.util.Map<K, V> map) { this.map = map; }
    public static <K extends Writable, V extends Writable>
        Map<K,V> of(java.util.Map<K,V> map) { return new Map(map); }


    @Override
    public void write(DataOutput out) throws IOException {
        if (map == null)
            return;

        // Build class index
        for (java.util.Map.Entry<K, V> entry: map.entrySet()) {
            index.addClass(entry.getKey().getClass());
            index.addClass(entry.getValue().getClass());
        }

        index.addClass(map.getClass());
        index.write(out);

        out.writeByte(index.getId(map.getClass()));
        out.writeInt(map.size());

        for (java.util.Map.Entry<K, V> entry: map.entrySet()) {
            out.writeByte(index.getId(entry.getKey().getClass()));
            entry.getKey().write(out);

            out.writeByte(index.getId(entry.getValue().getClass()));
            entry.getValue().write(out);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        // Read class index
        index.readFields(in);

        try {
            map = (java.util.Map<K, V>) index.getClass(in.readByte()).newInstance();

            for (int count = in.readInt(); count > 0; count --) {
                K key = (K) index.getClass(in.readByte()).newInstance();
                key.readFields(in);

                V val = (V) index.getClass(in.readByte()).newInstance();
                val.readFields(in);

                map.put(key, val);
            }
        } catch (Exception e) { throw new RuntimeException(e); }
    }
}
