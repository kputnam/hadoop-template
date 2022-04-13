package com.github.kputnam.hadoop.template.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * Created by kputnam on 3/3/14.
 */
public class Metrics {

    private static MetricRegistry registry;
    private static ScheduledReporter reporter;

    public static void report() {
        if (reporter == null)
            return;

        reporter.report();
    }

    public static void close() {
        if (registry == null || reporter == null)
            return;

        reporter.report();
        reporter.close();

        registry.removeMatching(MetricFilter.ALL);
        registry = null;
        reporter = null;
    }

    public static <T extends Metric> T gauge(String name, T metric) {
        return getRegistry().register(name, metric);
    }

    public static Counter counter(String name) {
        return getRegistry().counter(name);
    }

    public static Meter meter(String name) {
        return getRegistry().meter(name);
    }

    public static Histogram histogram(String name) {
        return getRegistry().histogram(name);
    }

    public static Timer timer(String name) {
        return getRegistry().timer(name);
    }

    private synchronized static MetricRegistry getRegistry() {
        if (registry != null)
            return registry;

        try {
            registry = new MetricRegistry();
            reporter = configureReporter(registry, loadConfig());
        } catch(FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        return registry;
    }

    public static ScheduledReporter configureReporter(MetricRegistry registry, Config config) {
        if (config.getGraphitePort() == -1 || config.getGraphiteHost() == null)
            return null;

        GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
                .filter(MetricFilter.ALL)
                .prefixedWith(config.getMetricPrefix())
                .convertRatesTo(config.getRateUnits())
                .convertDurationsTo(config.getDurationUnits())
                .build(new Graphite(new InetSocketAddress(
                        config.getGraphiteHost(), config.getGraphitePort())));

        reporter.start(config.getReportInterval(), TimeUnit.SECONDS);
        return reporter;
    }

    public static Config loadConfig() throws FileNotFoundException {
        return loadConfig("/metrics.yml");
    }

    public static Config loadConfig(String resourcePath) throws FileNotFoundException {
        Yaml yaml = new Yaml(new Constructor(Config.class));

        Config cfgYaml = null;
        Config cfgResult = new Config();

        // Read application-specific configuration (from classpath)
        InputStream inLocal = Metrics.class.getResourceAsStream(resourcePath);
        if (inLocal == null)
            throw new FileNotFoundException(resourcePath);

        cfgYaml = (Config) yaml.load(inLocal);
        if (cfgYaml == null)
            throw new IllegalArgumentException("Empty document: " + resourcePath);

        cfgResult.loadFrom(cfgYaml);

        // Read host-specific configuration
        if (cfgResult.getConfigPath() == null)
            return cfgResult;

        File cfgFile = new File(cfgResult.getConfigPath());
        if (!cfgFile.exists() || cfgFile.isDirectory())
            throw new FileNotFoundException(cfgResult.getConfigPath());

        cfgYaml = (Config) yaml.load(new FileInputStream(cfgFile));
        if (cfgYaml == null)
            throw new IllegalArgumentException("Empty document: " + cfgResult.getConfigPath());

        cfgResult.loadFrom(cfgYaml);
        System.out.println("Loaded metrics config: " + cfgResult);

        return cfgResult;
    }
}
