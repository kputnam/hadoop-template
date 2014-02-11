package com.github.kputnam.mapreduce.custom.v1;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The OutputFormat defines how results from a job are written back to
 * permanent storage (e.g. HDFS). Hadoop provides TextOutputFormat, which
 * writes (key, value) pairs each on their own line using #toString() and
 * also SequenceFileOutput which stores data in binary (to be read back
 * in using SequenceFileInput) using the Writable interface.
 *
 * This example generates a file like:
 *
 *   <results>
 *       <k1>v1</k1>
 *       <k2>v2</k2>
 *       ...
 *   </results>
 *
 * http://developer.yahoo.com/hadoop/tutorial/module5.html#outputformat
 */
public class CustomOutputFormat<K, V> extends FileOutputFormat<K, V> {

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem ignore, JobConf conf, String name, Progressable progress)
            throws IOException {
        Path file     = FileOutputFormat.getTaskOutputPath(conf, name);
        FileSystem fs = file.getFileSystem(conf);
        return new XmlRecordWriter<K, V>(fs.create(file, progress));
    }

    protected static class XmlRecordWriter<K, V> implements RecordWriter<K, V> {
        private DataOutputStream out;

        public XmlRecordWriter(DataOutputStream out) throws IOException {
            this.out = out;
            out.writeBytes("<results>\n");
        }

        @Override
        public void write(K key, V val) throws IOException {
            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullVal = val == null || val instanceof NullWritable;

            if (nullKey && nullVal)
                return;

            openKey(nullKey ? "value" : key);

            if (!nullVal)
                writeObject(val);

            closeKey(nullKey ? "value" : key);
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            out.writeBytes("</results>\n");
            out.close();
        }

        private void writeObject(Object o) throws IOException {
            if (o instanceof Text)
                out.write(((Text) o).getBytes(), 0, ((Text) o).getLength());
            else
                out.write(o.toString().getBytes("UTF-8"));
        }

        private void openKey(Object key) throws IOException {
            out.writeBytes("<");
            writeObject(key);
            out.writeBytes(">");
        }

        private void closeKey(Object key) throws IOException {
            out.writeBytes("</");
            writeObject(key);
            out.writeBytes(">\n");
        }
    }
}
