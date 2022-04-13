package com.github.kputnam.hadoop.template.metrics;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * Created by kputnam on 3/4/14.
 */
public class MetricsTest {

    @Test(expected = FileNotFoundException.class)
    public void testReadMissing() throws Exception {
        Metrics.loadConfig("/fixtures/metrics/missing.yml");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadEmpty() throws Exception {
        Metrics.loadConfig("/fixtures/metrics/empty.yml");
    }

    @Test
    public void testReadValid() throws Exception {
        Config config = Metrics.loadConfig("/fixtures/metrics/valid.yml");
        Assert.assertNull(config.getConfigPath());

        Assert.assertEquals("test",
                config.getMetricPrefix());

        Assert.assertEquals("graphite.example.com",
                config.getGraphiteHost());

        Assert.assertEquals(9999,
                config.getGraphitePort());

        Assert.assertEquals(TimeUnit.MILLISECONDS,
                config.getRateUnits());

        Assert.assertEquals(TimeUnit.MILLISECONDS,
                config.getDurationUnits());

        Assert.assertEquals(15,
                config.getReportInterval());
    }

    @Test
    public void testReadUnits() throws Exception {
        Config config = Metrics.loadConfig("/fixtures/metrics/units.yml");

        Assert.assertEquals(TimeUnit.SECONDS,
                config.getRateUnits());

        Assert.assertEquals(TimeUnit.HOURS,
                config.getDurationUnits());
    }

    @Test
    public void testReadSystemMissing() throws Exception {
        Assume.assumeTrue(!new File("/tmp/metrics.yml").exists());

        try {
            Config c = Metrics.loadConfig("/fixtures/metrics/system.yml");
            Assert.fail("Expected exception: java.io.FileNotFoundException");
        } catch(Exception e) {
            Assert.assertEquals(FileNotFoundException.class, e.getClass());
        }
    }

    @Test
    public void testReadSystemValid() throws Exception {
        Assume.assumeTrue(!new File("/tmp/metrics.yml").exists());

        PrintWriter w = new PrintWriter("/tmp/metrics.yml", "UTF-8");

        try {
            w.println("metricPrefix: test-xyz");
            w.println("graphiteHost: graphite.example.com");
            w.println("graphitePort: 9999");
            w.println("reportInterval: 30");
            w.close();

            Config config = Metrics.loadConfig("/fixtures/metrics/system.yml");

            Assert.assertEquals("/tmp/metrics.yml",
                    config.getConfigPath());

            Assert.assertEquals("test-xyz",
                    config.getMetricPrefix());

            Assert.assertEquals("graphite.example.com",
                    config.getGraphiteHost());

            Assert.assertEquals(9999,
                    config.getGraphitePort());

            Assert.assertEquals(30,
                    config.getReportInterval());

            Assert.assertEquals(TimeUnit.MICROSECONDS,
                    config.getRateUnits());

            Assert.assertEquals(TimeUnit.SECONDS,
                    config.getDurationUnits());
        } finally {
            new File("/tmp/metrics.yml").delete();
        }
    }
}
