package com.github.kputnam.hadoop.demo.metrics;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by kputnam on 2/27/14.
 */
public class InstrumentedMapper<InKey, InVal, OutKey, OutVal>
        extends Mapper<InKey, InVal, OutKey, OutVal> {

    @Override
    protected void cleanup(Context _) throws IOException, InterruptedException {
        Metrics.report();
    }
}
