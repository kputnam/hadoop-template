package com.github.kputnam.hadoop.demo.metrics;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by kputnam on 2/27/14.
 */
public class InstrumentedReducer<InKey, InVal, OutKey, OutVal>
        extends Reducer<InKey, InVal, OutKey, OutVal> {

    @Override
    protected void cleanup(Context _) throws IOException, InterruptedException {
        Metrics.report();
    }
}
