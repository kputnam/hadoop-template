package com.github.kputnam.mapreduce.custom.v1;

import com.github.kputnam.mapreduce.custom.CustomVal;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * The InputFormat defines both how to divide an input source into Splits
 * sent as a unit to an individual Mapper and also how to parse data from
 * that Split into individual records.
 *
 * http://developer.yahoo.com/hadoop/tutorial/module5.html#inputformat
 */
public class CustomInputFormat extends FileInputFormat<Text, CustomVal> {

    @Override
    public RecordReader<Text, CustomVal> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(split.toString());
        return new CustomRecordReader(job, (FileSplit) split);
    }

    /**
     * The record reader defines how to parse data from a single InputSplit into
     * individual records, sent to a Mapper. This example simply transforms each
     * line emitted by LineRecordReader.
     */
    public static class CustomRecordReader implements RecordReader<Text, CustomVal> {

        private LineRecordReader reader;
        private LongWritable lineKey;
        private Text lineVal;

        public CustomRecordReader(JobConf job, FileSplit split) throws IOException {
            reader  = new LineRecordReader(job, split);
            lineKey = reader.createKey();
            lineVal = reader.createValue();
        }

        @Override
        public boolean next(Text key, CustomVal val) throws IOException {
            if (!reader.next(lineKey, lineVal))
                return false;

            // Parse the lineVal which is formatted "name, x, y, z"
            String[] parts = lineVal.toString().split(", ");
            if (parts.length != 4)
                throw new IOException("Invalid record (expected 4 parts): " + lineVal);

            try {
                key.set(parts[0]);
                val.x = Float.parseFloat(parts[1]);
                val.y = Float.parseFloat(parts[2]);
                val.z = Float.parseFloat(parts[3]);
            } catch (NumberFormatException e) {
                throw new IOException("Invalid record (number format): " + lineVal);
            }

            return true;
        }

        @Override
        public Text createKey() {
            return new Text("");
        }

        @Override
        public CustomVal createValue() {
            return new CustomVal(0f, 0f, 0f);
        }

        @Override
        public long getPos() throws IOException {
            return reader.getPos();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }
    }

}
