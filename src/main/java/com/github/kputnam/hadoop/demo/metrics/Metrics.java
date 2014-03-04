package com.github.kputnam.hadoop.demo.metrics;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by kputnam on 3/3/14.
 */
public class Metrics {

    private static MetricRegistry registry;
    private static ScheduledReporter reporter;

    public static void close() {
        if (registry == null || reporter == null)
            return;

        reporter.report();
        reporter.close();
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

        registry = new MetricRegistry();
        reporter = configureGraphite(registry);

        return registry;
    }

    public static ScheduledReporter configureGraphite(MetricRegistry registry) {
        Config config;

        try { config = readConfiguration(); }
        catch (Exception e) { throw new RuntimeException(e); }

        if (config.graphitePort == -1 || config.graphiteHost == null)
            return null;

        GraphiteReporter reporter = GraphiteReporter.forRegistry(registry)
                .prefixedWith(config.getMetricPrefix())
                .filter(MetricFilter.ALL)
                .convertRatesTo(config.getRateUnits())
                .convertDurationsTo(config.getDurationUnits())
                .build(new Graphite(new InetSocketAddress(
                        config.getGraphiteHost(), config.getGraphitePort())));

        reporter.start(config.reportInterval, TimeUnit.SECONDS);
        return reporter;
    }

    private static Config readConfiguration() throws Exception {
        Yaml yaml = new Yaml(new Constructor(Config.class));

        Config config = new Config();

        // Search classpath for application-specific configuration
        InputStream inLocal = Metrics.class.getResourceAsStream("/metrics.yml");
        if (inLocal != null)
            ((Config) yaml.load(inLocal)).mergeInto(config);

        String cfgPath = config.getClusterConfig();

        // Look for host-specific configuration
        if (cfgPath != null) {
            File cfgFile = new File(cfgPath);

            if (!cfgFile.exists() || cfgFile.isDirectory())
                throw new FileNotFoundException("File not found: " + cfgPath);
            else
                config = config.mergeInto((Config) yaml.load(new FileInputStream(cfgFile)));
        }

        return config;
    }

    public static class Config {
        private String clusterConfig = "";
        private String metricPrefix = "";

        private int graphitePort = -1;
        private String graphiteHost = null;

        private int reportInterval = -1;
        private TimeUnit rateUnits = TimeUnit.MILLISECONDS;
        private TimeUnit durationUnits = TimeUnit.MILLISECONDS;

        private static final Map<String, TimeUnit> units = new HashMap<String, TimeUnit>();
        static {
            units.put("us", TimeUnit.MICROSECONDS);
            units.put("ms", TimeUnit.MILLISECONDS);
            units.put("s",  TimeUnit.SECONDS);
            units.put("m",  TimeUnit.MINUTES);
            units.put("hr", TimeUnit.HOURS);
            units.put("d",  TimeUnit.DAYS);
        }

        public String getClusterConfig() {
            return clusterConfig;
        }

        public void setClusterConfig(String clusterConfig) {
            this.clusterConfig = clusterConfig;
        }

        public int getGraphitePort() {
            return graphitePort;
        }

        public void setGraphitePort(int graphitePort) {
            this.graphitePort = graphitePort;
        }

        public String getGraphiteHost() {
            return graphiteHost;
        }

        public void setGraphiteHost(String graphiteHost) {
            this.graphiteHost = graphiteHost;
        }

        public String getMetricPrefix() {
            return metricPrefix;
        }

        public void setMetricPrefix(String metricPrefix) {
            this.metricPrefix = metricPrefix;
        }

        public int getReportInterval() {
            return reportInterval;
        }

        public void setReportInterval(int reportInterval) {
            this.reportInterval = reportInterval;
        }

        public TimeUnit getRateUnits() {
            return rateUnits;
        }

        public void setRateUnits(TimeUnit rateUnits) {
            this.rateUnits = rateUnits;
        }

        public void setRateUnits(String ratesUnit) {
            if (units.containsKey(ratesUnit))
                throw new IllegalArgumentException("Not a valid unit: '" + ratesUnit + "'");

            setRateUnits(units.get(ratesUnit));
        }

        public TimeUnit getDurationUnits() {
            return durationUnits;
        }

        public void setDurationUnits(TimeUnit durationUnits) {
            this.durationUnits = durationUnits;
        }

        public void setDurationUnits(String durationsUnit) {
            if (units.containsKey(rateUnits))
                throw new IllegalArgumentException("Not a valid unit: '" + rateUnits + "'");

            setDurationUnits(units.get(durationsUnit));
        }

        public Config mergeInto(Config that) {
            if (this.metricPrefix != null)
                that.setMetricPrefix(this.metricPrefix);

            if (this.graphitePort != -1)
                that.setGraphitePort(this.graphitePort);

            if (this.graphiteHost != null)
                that.setGraphiteHost(this.graphiteHost);

            if (this.reportInterval != -1)
                that.setReportInterval(this.reportInterval);

            if (this.rateUnits != null)
                that.setRateUnits(this.rateUnits);

            if (this.durationUnits != null)
                that.setDurationUnits(this.durationUnits);

            return that;
        }
    }
}
