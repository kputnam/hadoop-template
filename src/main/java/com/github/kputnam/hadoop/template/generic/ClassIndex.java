package com.github.kputnam.hadoop.template.generic;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kputnam on 2/20/14.
 */
public class ClassIndex implements Writable {

    /* Map class to id */
    private Map<Class, Byte> classToId = new ConcurrentHashMap<Class, Byte>();

    /* Map id to Class */
    private Map<Byte, Class> idToClass = new ConcurrentHashMap<Byte, Class>();

    /* Number of new classes */
    private volatile byte classCount = 0;

    public ClassIndex() { };

    public byte getId(Class klazz) {
        return classToId.get(klazz);
    }

    public Class getClass(byte id) {
        return idToClass.get(id);
    }

    public synchronized void addClass(Class klass) {
        if (classToId.containsKey(klass))
            return;

        if (classCount == Byte.MAX_VALUE)
            throw new IndexOutOfBoundsException("Too many distinct classes");

        addClass(klass, classCount++);
    }

    private synchronized void addClass(Class klass, byte id) {
        if (classToId.containsKey(klass) && id != classToId.get(klass))
            throw new IllegalArgumentException("Class " + klass.getName() +
                " is already mapped to a different id");

        if (idToClass.containsKey(id) && !idToClass.get(id).equals(klass))
            throw new IllegalArgumentException("Id " + id +
                " is already mapped to a different class");

        classToId.put(klass, id);
        idToClass.put(id, klass);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(classCount);

        for (byte k = 0; k < classCount; k ++)
            out.writeUTF(getClass(k).getName());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        classCount = 0;
        classToId.clear();
        idToClass.clear();

        for (byte k = in.readByte(); k > 0; k --)
            try { addClass(Class.forName(in.readUTF())); }
            catch (ClassNotFoundException e) { throw new RuntimeException(e); }
    }
}
