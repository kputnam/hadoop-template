package com.github.kputnam.hadoop.demo.metrics;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by kputnam on 2/27/14.
 */
public class InstrumentedReducer<InKey, InVal, OutKey, OutVal>
        extends Reducer<InKey, InVal, OutKey, OutVal> {

    protected final MetricRegistry metrics = new MetricRegistry();
    protected final GraphiteReporter reporter =
            GraphiteReporter.forRegistry(metrics)
                    .prefixedWith("hadoop-demo")
                    .convertRatesTo(TimeUnit.MILLISECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(new Graphite(new InetSocketAddress("127.0.0.1", 2003)));

    @Override
    protected void setup(Context _) throws IOException, InterruptedException {
        reporter.start(5, TimeUnit.SECONDS);
    }

    @Override
    protected void cleanup(Context _) throws IOException, InterruptedException {
        reporter.report();
        reporter.close();
    }
}
