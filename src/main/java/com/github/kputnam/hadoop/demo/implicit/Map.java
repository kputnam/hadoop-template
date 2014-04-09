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
public abstract class Map<K extends Writable,
                          V extends Writable>
        implements Writable {

    public java.util.Map<K, V> map;

    public Map() { }

    public Map(java.util.Map<K, V> map) { this.map = map; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(map.size());

        for (java.util.Map.Entry<K, V> entry: map.entrySet()) {
            entry.getKey().write(out);
            entry.getValue().write(out);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        try {
            map = (java.util.Map<K, V>) getMapClass().newInstance();

            for (int count = in.readInt(); count > 0; count --) {
                K key = (K) getClassK().newInstance();
                key.readFields(in);

                V val = (V) getClassV().newInstance();
                val.readFields(in);

                map.put(key, val);
            }
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    protected abstract Class<Map<K,V>> getMapClass();

    @SuppressWarnings("unchecked")
    private Class<K> getClassK() {
        Type klass = getClass().getGenericSuperclass();
        Type param = ((ParameterizedType) klass).getActualTypeArguments()[0];
        return (Class<K>) param;
    }

    @SuppressWarnings("unchecked")
    private Class<V> getClassV() {
        Type klass = getClass().getGenericSuperclass();
        Type param = ((ParameterizedType) klass).getActualTypeArguments()[1];
        return (Class<V>) param;
    }
}
