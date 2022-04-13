package com.github.kputnam.hadoop.template.custom.v1;

import com.github.kputnam.hadoop.template.custom.CustomVal;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;

/**
 * The InputFormat defines both how to divide an input source into Splits
 * sent whole to an individual Mapper and how to read data from that Split
 * into individual records.
 *
 * Typically, a Split corresponds to a physical HDFS block and its locations
 * are the set of nodes where the block is physically located. It's possible
 * to create a different implementation of Split that assigns locations in a
 * round-robin fashion, for instance.
 *
 * http://developer.yahoo.com/hadoop/tutorial/module5.html#inputformat
 */
public class CustomInputSplit extends FileInputFormat<Text, CustomVal> {

    private static final double SPLIT_SLOP = 1.1; // 10%
    private static final String NUM_INPUTS = "mapreduce.input.num.files";
    private static final String MIN_SPLIT_SIZE = "mapred.min.split.size";
    private long minSplitSize = 1;

    @Override
    protected void setMinSplitSize(long minSplitSize) {
        this.minSplitSize = minSplitSize;
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        FileStatus[] files      = listStatus(job);
        List<InputSplit> splits = new ArrayList<InputSplit>();

        long totalSize  = 0;
        for (FileStatus file: files)
            totalSize += file.getLen();

        long minSize    = Math.max(job.getLong(MIN_SPLIT_SIZE, 1), minSplitSize);
        long maxSize    = totalSize / (numSplits == 0 ? 1 : numSplits);

        String[] nodes  = getActiveNodes(job);
        int currentNode = 0;

        for (FileStatus file: files) {
            long fileLength = file.getLen();
            Path filePath   = file.getPath();

            if (fileLength == 0) {
                // Create an empty location array for empty files
                splits.add(new FileSplit(file.getPath(), 0, fileLength, new String[0]));
            } else if (!isSplitable(filePath.getFileSystem(job), filePath)) {
                // File can't be divided, send the whole thing to the next node
                splits.add(new FileSplit(filePath, 0, fileLength,
                        new String[] { nodes[currentNode++ % nodes.length] }));
            } else {
                // Assign each block a single location in round-robin order (entirely
                // ignoring data locality in HDFS...).
                long blockSize = file.getBlockSize();
                long splitSize = computeSplitSize(maxSize, minSize, blockSize);
                long remaining = fileLength;

                while (((double) remaining)/splitSize > SPLIT_SLOP) {
                    splits.add(new FileSplit(filePath, fileLength-remaining,
                            splitSize, new String[] { nodes[currentNode++ % nodes.length] }));
                    remaining -= splitSize;
                }

                if (remaining > 0)
                    splits.add(new FileSplit(filePath, fileLength-remaining,
                            remaining, new String[] { nodes[currentNode] }));
            }
        }

        // Record number of input files
        job.setLong(NUM_INPUTS, files.length);

        return splits.toArray(new FileSplit[splits.size()]);
    }

    private String[] getActiveNodes(JobConf job) {
        ClusterStatus status;

        try {
            status = new JobClient(job).getClusterStatus(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Collection<String> infos = status.getActiveTrackerNames();
        String[] nodes = new String[infos.size()];
        int k = 0;

        for (String info: infos) {
            StringTokenizer a = new StringTokenizer(info, ":");
            StringTokenizer b = new StringTokenizer(a.nextToken(), "_");
            b.nextToken(); // Skip something
            nodes[k ++] = b.nextToken();
        }

        return nodes;
    }

    @Override
    public RecordReader<Text, CustomVal> getRecordReader(org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(split.toString());
        return new CustomInputFormat.CustomRecordReader(job, (FileSplit) split);
    }

}
